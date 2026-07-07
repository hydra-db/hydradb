use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{
    local_object_store, object_store_from_env, ArtifactDirection, EdgeMutation, GraphCacheConfig,
    GraphCachePolicy, GraphLimits, GraphOpenOptions, GraphShard, QueryContext, QueryResultPage,
    QueryResultSet, QueryValue,
};

type CheckResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CELL_ID: &str = "reddit-home";
const EDGE_TYPE: &str = "USER_FOLLOWS_USER";
const DEFAULT_FANOUTS: &[u64] = &[50, 100, 1_000];
const DEFAULT_HOPS: &[u8] = &[1, 5, 10, 20];

struct CheckRecord<'a> {
    object_backend: &'a str,
    fanout: u64,
    hops: u8,
    check: &'a str,
    repeats: usize,
    expected_rows: usize,
    actual_rows: usize,
    elapsed_us: u128,
}

#[tokio::main]
async fn main() -> CheckResult<()> {
    let fanouts = parse_u64_list("GRAPH_QUERY_CORRECTNESS_FANOUTS", DEFAULT_FANOUTS);
    let hops = parse_u8_list("GRAPH_QUERY_CORRECTNESS_HOPS", DEFAULT_HOPS);
    let max_hop = env_u8(
        "GRAPH_QUERY_CORRECTNESS_DATA_HOPS",
        hops.iter().copied().max().unwrap_or(20),
    );
    let repeats = env_usize("GRAPH_QUERY_CORRECTNESS_REPEATS", 3).max(1);
    let page_size = env_usize("GRAPH_QUERY_CORRECTNESS_PAGE_SIZE", 64).max(1);
    let tile_size = env_u64("GRAPH_QUERY_CORRECTNESS_MATRIX_TILE", 4_096);
    let bulk_chunk_size = env_usize("GRAPH_QUERY_CORRECTNESS_BULK_CHUNK_SIZE", 100_000);

    let root = TempCheckRoot::new()?;
    let cache_root = root.path().join("slatedb-cache");
    fs::create_dir_all(&cache_root)?;
    let (object_store, object_backend) =
        if let Ok(env_file) = std::env::var("GRAPH_QUERY_CORRECTNESS_OBJECT_ENV") {
            (object_store_from_env(Some(env_file))?, "env")
        } else {
            let object_root = root.path().join("object-store");
            fs::create_dir_all(&object_root)?;
            (local_object_store(&object_root)?, "local")
        };
    let run_id = format!("query-correctness-{}", std::process::id());
    println!(
        "object_backend,fanout,hops,expected_reachable,check,repeats,expected_rows,actual_rows,elapsed_us,status"
    );

    for fanout in fanouts {
        let shard_path = format!("{run_id}/fanout-{fanout}");
        let writer = GraphShard::open_standalone_writer_with_options(
            shard_path.clone(),
            Arc::clone(&object_store),
            graph_options(None, fanout, max_hop),
        )
        .await?;
        writer
            .bulk_import_edges_chunked(
                CELL_ID,
                EDGE_TYPE,
                layered_edges(fanout, max_hop),
                &format!("correctness-fanout-{fanout}"),
                bulk_chunk_size,
            )
            .await?;
        let base_epoch = writer.current_epoch(CELL_ID).await?;
        writer
            .build_matrix_tiles(CELL_ID, EDGE_TYPE, base_epoch, tile_size)
            .await?;
        writer
            .build_supernode_groups_for_directions(
                CELL_ID,
                EDGE_TYPE,
                base_epoch,
                10,
                512,
                &[ArtifactDirection::Out],
            )
            .await?;
        writer
            .refresh_edge_type_query_stats(CELL_ID, EDGE_TYPE)
            .await?;
        writer.close().await?;

        let cache_dir = cache_root.join(format!("fanout-{fanout}"));
        reset_dir(&cache_dir)?;
        let reader = GraphShard::open_with_options(
            shard_path,
            Arc::clone(&object_store),
            graph_options(Some(&cache_dir), fanout, max_hop),
        )
        .await?;

        verify_supernode_page(&reader, object_backend, fanout, page_size, repeats).await?;
        for hop in &hops {
            verify_full_rows(&reader, object_backend, fanout, *hop, repeats).await?;
            verify_count(&reader, object_backend, fanout, *hop, repeats).await?;
            verify_pages(&reader, object_backend, fanout, *hop, page_size, repeats).await?;
        }
        reader.close().await?;
    }

    verify_epoch_invalidation(Arc::clone(&object_store), object_backend, &run_id).await?;
    Ok(())
}

async fn verify_supernode_page(
    shard: &GraphShard,
    object_backend: &str,
    fanout: u64,
    page_size: usize,
    repeats: usize,
) -> CheckResult<()> {
    let query = format!("MATCH (u {{id: 1}})-[:{EDGE_TYPE}]->(v) RETURN v.id ORDER BY v.id");
    let expected = first_page(expected_vertices(fanout, 1), page_size);
    let mut actual = Vec::new();
    let started = Instant::now();
    for repeat in 0..repeats {
        let page = shard
            .execute_cypher_rows_page(
                QueryContext::new(CELL_ID, format!("correct-supernode-page-{fanout}-{repeat}")),
                &query,
                None,
                page_size,
            )
            .await?;
        actual = page_vertices(&page)?;
        assert_exact(
            "supernode_page",
            fanout,
            1,
            &expected,
            &actual,
            page.next_cursor.is_some() == (fanout as usize > page_size),
        )?;
    }
    print_check(CheckRecord {
        object_backend,
        fanout,
        hops: 1,
        check: "supernode_page",
        repeats,
        expected_rows: expected.len(),
        actual_rows: actual.len(),
        elapsed_us: started.elapsed().as_micros(),
    });
    Ok(())
}

async fn verify_full_rows(
    shard: &GraphShard,
    object_backend: &str,
    fanout: u64,
    hop: u8,
    repeats: usize,
) -> CheckResult<()> {
    let query = format!("MATCH (u {{id: 1}})-[:{EDGE_TYPE}*1..{hop}]->(v) RETURN v.id");
    let expected = expected_vertices(fanout, hop);
    let expected_set: BTreeSet<_> = expected.iter().copied().collect();
    let mut actual = Vec::new();
    let started = Instant::now();
    for repeat in 0..repeats {
        let rows = shard
            .execute_cypher_rows(
                QueryContext::new(
                    CELL_ID,
                    format!("correct-full-rows-{fanout}-{hop}-{repeat}"),
                ),
                &query,
            )
            .await?;
        actual = row_vertices(&rows)?;
        let actual_set: BTreeSet<_> = actual.iter().copied().collect();
        if actual.len() != expected.len() || actual_set != expected_set {
            return Err(format!(
                "full rows mismatch fanout={fanout} hop={hop}: expected {} unique rows, got {} rows and {} unique rows",
                expected.len(),
                actual.len(),
                actual_set.len()
            )
            .into());
        }
    }
    print_check(CheckRecord {
        object_backend,
        fanout,
        hops: hop,
        check: "multi_hop_rows_exact_set",
        repeats,
        expected_rows: expected.len(),
        actual_rows: actual.len(),
        elapsed_us: started.elapsed().as_micros(),
    });
    Ok(())
}

async fn verify_count(
    shard: &GraphShard,
    object_backend: &str,
    fanout: u64,
    hop: u8,
    repeats: usize,
) -> CheckResult<()> {
    let query =
        format!("MATCH (u {{id: 1}})-[:{EDGE_TYPE}*1..{hop}]->(v) RETURN count(*) AS total");
    let expected = fanout.saturating_mul(u64::from(hop));
    let mut actual = 0;
    let started = Instant::now();
    for repeat in 0..repeats {
        let rows = shard
            .execute_cypher_rows(
                QueryContext::new(CELL_ID, format!("correct-count-{fanout}-{hop}-{repeat}")),
                &query,
            )
            .await?;
        actual = single_count(&rows)?;
        if actual != expected {
            return Err(format!(
                "count mismatch fanout={fanout} hop={hop}: expected {expected}, got {actual}"
            )
            .into());
        }
    }
    print_check(CheckRecord {
        object_backend,
        fanout,
        hops: hop,
        check: "multi_hop_count_exact",
        repeats,
        expected_rows: 1,
        actual_rows: usize::from(actual > 0),
        elapsed_us: started.elapsed().as_micros(),
    });
    Ok(())
}

async fn verify_pages(
    shard: &GraphShard,
    object_backend: &str,
    fanout: u64,
    hop: u8,
    page_size: usize,
    repeats: usize,
) -> CheckResult<()> {
    let query =
        format!("MATCH (u {{id: 1}})-[:{EDGE_TYPE}*1..{hop}]->(v) RETURN v.id ORDER BY v.id");
    let expected = expected_vertices(fanout, hop);
    let first_expected = first_page(expected.clone(), page_size);
    let second_expected = page_slice(&expected, page_size, page_size);
    let started = Instant::now();
    let mut actual_total = 0;
    for repeat in 0..repeats {
        let first = shard
            .execute_cypher_rows_page(
                QueryContext::new(CELL_ID, format!("correct-page-1-{fanout}-{hop}-{repeat}")),
                &query,
                None,
                page_size,
            )
            .await?;
        let first_actual = page_vertices(&first)?;
        assert_exact(
            "multi_hop_page_first",
            fanout,
            hop,
            &first_expected,
            &first_actual,
            first.next_cursor.is_some() == (expected.len() > page_size),
        )?;
        actual_total = first_actual.len();

        if let Some(cursor) = first.next_cursor {
            let second = shard
                .execute_cypher_rows_page(
                    QueryContext::new(CELL_ID, format!("correct-page-2-{fanout}-{hop}-{repeat}")),
                    &query,
                    Some(cursor),
                    page_size,
                )
                .await?;
            let second_actual = page_vertices(&second)?;
            assert_exact(
                "multi_hop_page_second",
                fanout,
                hop,
                &second_expected,
                &second_actual,
                second.next_cursor.is_some() == (expected.len() > page_size * 2),
            )?;
            actual_total += second_actual.len();
        }
    }
    print_check(CheckRecord {
        object_backend,
        fanout,
        hops: hop,
        check: "multi_hop_page_exact_order",
        repeats,
        expected_rows: first_expected.len().saturating_add(second_expected.len()),
        actual_rows: actual_total,
        elapsed_us: started.elapsed().as_micros(),
    });
    Ok(())
}

async fn verify_epoch_invalidation(
    object_store: Arc<dyn ObjectStore>,
    object_backend: &str,
    run_id: &str,
) -> CheckResult<()> {
    let fanout = 8;
    let max_hop = 3;
    let path = format!("{run_id}/epoch-invalidation");
    let shard = GraphShard::open_standalone_writer_with_options(
        path,
        object_store,
        graph_options(None, fanout, max_hop),
    )
    .await?;
    shard
        .bulk_import_edges_chunked(
            CELL_ID,
            EDGE_TYPE,
            layered_edges(fanout, max_hop),
            "correctness-epoch-base",
            1_024,
        )
        .await?;
    let base_epoch = shard.current_epoch(CELL_ID).await?;
    shard
        .build_matrix_tiles(CELL_ID, EDGE_TYPE, base_epoch, 128)
        .await?;

    let query = format!("MATCH (u {{id: 1}})-[:{EDGE_TYPE}*1..1]->(v) RETURN v.id ORDER BY v.id");
    let before = shard
        .execute_cypher_rows(
            QueryContext::new(CELL_ID, "correctness-epoch-before"),
            &query,
        )
        .await?;
    let mut before_vertices = row_vertices(&before)?;
    let expected_before = expected_vertices(fanout, 1);
    assert_exact(
        "epoch_before",
        fanout,
        1,
        &expected_before,
        &before_vertices,
        true,
    )?;

    let new_vertex = 9_999_999;
    shard
        .write_edge(EdgeMutation {
            cell_id: CELL_ID.to_string(),
            edge_type: EDGE_TYPE.to_string(),
            src: 1,
            dst: new_vertex,
            idempotency_key: "correctness-epoch-new-edge".to_string(),
        })
        .await?;
    let after = shard
        .execute_cypher_rows(
            QueryContext::new(CELL_ID, "correctness-epoch-after"),
            &query,
        )
        .await?;
    let mut expected_after = expected_before;
    expected_after.push(new_vertex);
    expected_after.sort_unstable();
    before_vertices = row_vertices(&after)?;
    assert_exact(
        "epoch_after",
        fanout,
        1,
        &expected_after,
        &before_vertices,
        true,
    )?;
    print_check(CheckRecord {
        object_backend,
        fanout,
        hops: 1,
        check: "epoch_cache_invalidation",
        repeats: 1,
        expected_rows: expected_after.len(),
        actual_rows: before_vertices.len(),
        elapsed_us: 0,
    });
    shard.close().await?;
    Ok(())
}

fn row_vertices(rows: &QueryResultSet) -> CheckResult<Vec<u64>> {
    let mut vertices = Vec::with_capacity(rows.rows.len());
    for row in &rows.rows {
        match row.values.as_slice() {
            [QueryValue::VertexId(vertex)] => vertices.push(*vertex),
            values => return Err(format!("expected one vertex id column, got {values:?}").into()),
        }
    }
    Ok(vertices)
}

fn page_vertices(page: &QueryResultPage) -> CheckResult<Vec<u64>> {
    let rows = QueryResultSet::new(page.columns.clone(), page.rows.clone());
    row_vertices(&rows)
}

fn single_count(rows: &QueryResultSet) -> CheckResult<u64> {
    match rows.rows.as_slice() {
        [row] => match row.values.as_slice() {
            [QueryValue::Count(count)] => Ok(*count),
            values => Err(format!("expected count value, got {values:?}").into()),
        },
        other => Err(format!("expected one count row, got {}", other.len()).into()),
    }
}

fn assert_exact(
    check: &str,
    fanout: u64,
    hop: u8,
    expected: &[u64],
    actual: &[u64],
    cursor_ok: bool,
) -> CheckResult<()> {
    if expected != actual || !cursor_ok {
        return Err(format!(
            "{check} mismatch fanout={fanout} hop={hop}: expected {:?}, got {:?}, cursor_ok={cursor_ok}",
            preview(expected),
            preview(actual)
        )
        .into());
    }
    Ok(())
}

fn preview(values: &[u64]) -> Vec<u64> {
    let mut out: Vec<_> = values.iter().take(8).copied().collect();
    if values.len() > 8 {
        out.push(u64::MAX);
        out.extend(
            values
                .iter()
                .rev()
                .take(3)
                .copied()
                .collect::<Vec<_>>()
                .into_iter()
                .rev(),
        );
    }
    out
}

fn expected_vertices(fanout: u64, hop: u8) -> Vec<u64> {
    let mut vertices = Vec::with_capacity(fanout.saturating_mul(u64::from(hop)) as usize);
    for depth in 1..=hop {
        for index in 0..fanout {
            vertices.push(layer_vertex(depth, index));
        }
    }
    vertices.sort_unstable();
    vertices
}

fn first_page(mut vertices: Vec<u64>, page_size: usize) -> Vec<u64> {
    vertices.truncate(page_size);
    vertices
}

fn page_slice(vertices: &[u64], skip: usize, limit: usize) -> Vec<u64> {
    vertices.iter().skip(skip).take(limit).copied().collect()
}

fn graph_options(cache_dir: Option<&Path>, fanout: u64, max_hop: u8) -> GraphOpenOptions {
    let edges = layered_edge_count(fanout, max_hop);
    let query_rows = edges.saturating_add(fanout).saturating_add(1_024);
    GraphOpenOptions {
        limits: GraphLimits {
            max_bulk_import_edges: usize::try_from(edges).unwrap_or(usize::MAX).max(1),
            max_artifact_source_epochs: u64::MAX,
            max_traversal_hops: max_hop,
            max_artifact_build_edges: edges.saturating_add(1),
            max_query_result_vertices: usize::try_from(query_rows).unwrap_or(usize::MAX),
            max_query_intermediate_rows: usize::try_from(query_rows).unwrap_or(usize::MAX),
            max_query_index_candidates: usize::try_from(query_rows).unwrap_or(usize::MAX),
            max_query_scan_edges: edges.saturating_mul(u64::from(max_hop).max(1)).max(1),
            max_query_runtime_ms: Some(env_u64("GRAPH_QUERY_CORRECTNESS_TIMEOUT_MS", 120_000)),
        },
        cache: cache_dir
            .map(|path| GraphCacheConfig::disk_cache_without_preload(path, 512 * 1024 * 1024))
            .unwrap_or_default(),
        cache_policy: GraphCachePolicy {
            max_matrix_adjacencies: 8,
            max_graphblas_matrices: 8,
            max_posting_chunks: 262_144,
            max_entries_per_cell: None,
            prefetch_supernode_chunks: 2,
            max_concurrent_hydrations: 16,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn print_check(record: CheckRecord<'_>) {
    println!(
        "{},{},{},{},{},{},{},{},{},ok",
        record.object_backend,
        record.fanout,
        record.hops,
        layered_edge_count(record.fanout, record.hops),
        record.check,
        record.repeats,
        record.expected_rows,
        record.actual_rows,
        record.elapsed_us
    );
}

fn layered_edges(fanout: u64, max_hop: u8) -> impl Iterator<Item = (u64, u64)> {
    (0..fanout).flat_map(move |index| {
        (1..=max_hop).scan(1, move |src, hop| {
            let dst = layer_vertex(hop, index);
            let edge = (*src, dst);
            *src = dst;
            Some(edge)
        })
    })
}

fn layered_edge_count(fanout: u64, max_hop: u8) -> u64 {
    fanout.saturating_mul(u64::from(max_hop))
}

fn layer_vertex(hop: u8, index: u64) -> u64 {
    (u64::from(hop) * 1_000_000) + index + 1
}

fn parse_u64_list(name: &str, default: &[u64]) -> Vec<u64> {
    let Ok(value) = std::env::var(name) else {
        return default.to_vec();
    };
    let parsed: Vec<_> = value
        .split(',')
        .filter_map(|item| item.trim().parse().ok())
        .filter(|value| *value > 0)
        .collect();
    if parsed.is_empty() {
        default.to_vec()
    } else {
        parsed
    }
}

fn parse_u8_list(name: &str, default: &[u8]) -> Vec<u8> {
    let Ok(value) = std::env::var(name) else {
        return default.to_vec();
    };
    let parsed: Vec<_> = value
        .split(',')
        .filter_map(|item| item.trim().parse().ok())
        .filter(|value| *value > 0)
        .collect();
    if parsed.is_empty() {
        default.to_vec()
    } else {
        parsed
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u8(name: &str, default: u8) -> u8 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn reset_dir(path: &Path) -> CheckResult<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    Ok(())
}

struct TempCheckRoot {
    path: PathBuf,
}

impl TempCheckRoot {
    fn new() -> CheckResult<Self> {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let path =
            std::env::temp_dir().join(format!("query-correctness-{}-{stamp}", std::process::id()));
        reset_dir(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempCheckRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
