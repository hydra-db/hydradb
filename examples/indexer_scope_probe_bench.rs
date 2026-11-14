//! Read-only benchmark for the graph indexer's clean-scope recovery sweep.
//!
//! The harness opens existing scoped readers, establishes their covered
//! storage sequences, then compares the previous sweep shape with the current
//! refresh-only fast path at several concurrency levels. It never builds,
//! publishes, deletes, acknowledges, or advances an indexer cursor.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use futures::{StreamExt, TryStreamExt};
use hydradb::{
    object_store_from_env, GraphCluster, GraphId, GraphOpenOptions, GraphReaderMode, GraphScope, NamespacePath,
    ObjectStoreGraphScopeDirectory, Result,
};
use slatedb::object_store::path::Path;
use slatedb::object_store::ObjectStore;

struct OpenScope {
    scope: GraphScope,
    cluster: Arc<GraphCluster>,
    covered: BTreeMap<String, u64>,
}

fn env_value(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn parse_concurrencies() -> Vec<usize> {
    env_value("BENCH_CONCURRENCIES", "16,32,64")
        .split(',')
        .filter_map(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .collect()
}

fn env_flag(name: &str, default: bool) -> bool {
    std::env::var(name)
        .map(|value| matches!(value.trim(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(default)
}

fn graph_scope() -> Result<GraphScope> {
    let namespace = NamespacePath::parse(&env_value("GRAPH_NAMESPACE", "default"))?;
    Ok(GraphScope::new(namespace, GraphId::new(env_value("GRAPH_ID", "default"))?))
}

async fn scope_has_data(data_path: &str, scope: &GraphScope, cells: &[String], object_store: &Arc<dyn ObjectStore>) -> Result<bool> {
    let scope_path = scope.scoped_store_path(data_path);
    for cell_id in cells {
        let prefix = Path::from(format!("{scope_path}/{cell_id}"));
        if object_store.list(Some(&prefix)).next().await.transpose()?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn open_scope(
    data_path: String,
    scope: GraphScope,
    cells: Vec<String>,
    object_store: Arc<dyn ObjectStore>,
    options: GraphOpenOptions,
) -> Result<Option<OpenScope>> {
    if !scope_has_data(&data_path, &scope, &cells, &object_store).await? {
        return Ok(None);
    }
    let cluster =
        Arc::new(GraphCluster::open_cells_scoped_with_options(data_path, scope.clone(), cells.clone(), object_store, options).await?);
    let mut covered = BTreeMap::new();
    for cell_id in cells {
        let sequence = cluster
            .shard(&cell_id)
            .expect("opened cluster contains every configured cell")
            .refresh_storage_sequence(&cell_id)
            .await?;
        covered.insert(cell_id, sequence);
    }
    Ok(Some(OpenScope { scope, cluster, covered }))
}

async fn legacy_probe(open: &OpenScope, data_path: &str, cells: &[String], object_store: &Arc<dyn ObjectStore>) -> Result<()> {
    if !scope_has_data(data_path, &open.scope, cells, object_store).await? {
        return Ok(());
    }
    for cell_id in cells {
        let shard = open.cluster.shard(cell_id).expect("opened cluster contains every configured cell");
        shard.refresh_storage_sequence(cell_id).await?;
        for (edge_type, _) in shard.dirty_graph_index_edge_types(cell_id).await? {
            let _ = shard.current_graph_index(cell_id, &edge_type).await?;
        }
    }
    Ok(())
}

async fn refresh_only_probe(open: &OpenScope, cells: &[String]) -> Result<()> {
    for cell_id in cells {
        let sequence = open
            .cluster
            .shard(cell_id)
            .expect("opened cluster contains every configured cell")
            .refresh_storage_sequence(cell_id)
            .await?;
        if open.covered.get(cell_id) != Some(&sequence) {
            return Err(hydradb::GraphError::CorruptValue {
                key: open.scope.to_string(),
                reason: format!(
                    "scope changed during read-only benchmark: covered {:?}, refreshed {sequence}",
                    open.covered.get(cell_id)
                ),
            });
        }
    }
    Ok(())
}

fn print_measurement(label: &str, scopes: usize, concurrency: usize, started: Instant) {
    let elapsed = started.elapsed();
    let rate = scopes as f64 / elapsed.as_secs_f64();
    println!(
        "{label:<20} concurrency={concurrency:<3} scopes={scopes:<4} wall_ms={:<8} scopes_per_s={rate:.2}",
        elapsed.as_millis(),
    );
}

async fn measure_legacy(
    scopes: &[OpenScope],
    concurrency: usize,
    data_path: &str,
    cells: &[String],
    object_store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let started = Instant::now();
    futures::stream::iter(scopes)
        .map(|open| legacy_probe(open, data_path, cells, object_store))
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    print_measurement("legacy warm sweep", scopes.len(), concurrency, started);
    Ok(())
}

async fn measure_refresh_only(scopes: &[OpenScope], concurrency: usize, cells: &[String]) -> Result<()> {
    let started = Instant::now();
    futures::stream::iter(scopes)
        .map(|open| refresh_only_probe(open, cells))
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    print_measurement("refresh-only sweep", scopes.len(), concurrency, started);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let data_path = env_value("GRAPH_DATA_PATH", "graph/data");
    let root_scope = graph_scope()?;
    let cells = env_value("GRAPH_CELLS", "cell-0")
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let limit = env_value("BENCH_SCOPES", "512").parse::<usize>().unwrap_or(512);
    let open_concurrency = env_value("BENCH_OPEN_CONCURRENCY", "32").parse::<usize>().unwrap_or(32).max(1);
    let object_store = object_store_from_env(None)?;
    let directory = ObjectStoreGraphScopeDirectory::new(
        data_path.clone(),
        root_scope.namespace.clone(),
        root_scope.graph_id.clone(),
        Arc::clone(&object_store),
    );
    let scopes = directory.list().await?.into_iter().take(limit).collect::<Vec<_>>();
    println!(
        "opening {} staging scopes read-only with concurrency {open_concurrency}",
        scopes.len()
    );

    let options = GraphOpenOptions::default();
    let options = GraphOpenOptions {
        reader_mode: GraphReaderMode::FollowLatest,
        reader_manifest_poll_interval: std::time::Duration::from_secs(24 * 60 * 60),
        ..options
    };
    let started = Instant::now();
    let opened = futures::stream::iter(scopes)
        .map(|scope| open_scope(data_path.clone(), scope, cells.clone(), Arc::clone(&object_store), options.clone()))
        .buffer_unordered(open_concurrency)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    println!(
        "opened {} non-empty scopes in {} ms; timed passes start now",
        opened.len(),
        started.elapsed().as_millis()
    );

    let measure_legacy_sweep = env_flag("BENCH_LEGACY", true);
    for concurrency in parse_concurrencies() {
        if measure_legacy_sweep {
            measure_legacy(&opened, concurrency, &data_path, &cells, &object_store).await?;
        }
        measure_refresh_only(&opened, concurrency, &cells).await?;
    }

    for open in opened {
        open.cluster.close().await?;
    }
    Ok(())
}
