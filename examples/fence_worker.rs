use slatedb::object_store::ObjectStore;
use slatedb::{CloseReason, ErrorKind};
use slatedb_graph_kernel::{
    local_object_store, object_store_from_env, EdgeMutation, GraphError, GraphIndexPolicy,
    GraphOpenOptions, GraphShard, Result,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CELL_ID: &str = "reddit-home";
const EDGE_TYPE: &str = "USER_FOLLOWS_USER";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        eprintln!(
            "usage: fence_worker <incumbent|takeover|reader> <env-file|local:PATH|-> <base-path> <signal-dir>"
        );
        std::process::exit(2);
    }

    let mode = args[1].as_str();
    let object_store = load_object_store(&args[2])?;
    let data_path = format!("{}/{CELL_ID}", args[3]);
    let signal_dir = PathBuf::from(&args[4]);
    std::fs::create_dir_all(&signal_dir).map_err(io_error)?;

    match mode {
        "incumbent" => incumbent(object_store, &data_path, &signal_dir).await?,
        "takeover" => takeover(object_store, &data_path, &signal_dir).await?,
        "reader" => reader(object_store, &data_path).await?,
        other => {
            eprintln!("unknown mode {other}");
            std::process::exit(2);
        }
    }
    Ok(())
}

async fn incumbent(
    object_store: Arc<dyn ObjectStore>,
    data_path: &str,
    signals: &Path,
) -> Result<()> {
    let shard = GraphShard::open_standalone_writer_with_options(
        data_path.to_string(),
        object_store,
        graph_options(),
    )
    .await?;
    shard.write_edge(mutation(100, 10, "incumbent-10")).await?;
    signal(signals, "incumbent-ready")?;
    wait_for_signal(signals, "takeover-committed").await?;

    match shard.write_edge(mutation(100, 777, "stale-777")).await {
        Err(GraphError::Slate(error))
            if matches!(error.kind(), ErrorKind::Closed(CloseReason::Fenced)) =>
        {
            signal(signals, "stale-rejected")?;
            println!("SlateDB rejected the incumbent writer after takeover");
            Ok(())
        }
        Err(error) => Err(GraphError::CorruptValue {
            key: "fence/incumbent".to_string(),
            reason: format!("expected SlateDB fenced error, got {error}"),
        }),
        Ok(_) => Err(GraphError::CorruptValue {
            key: "fence/incumbent".to_string(),
            reason: "stale writer committed after replacement opened".to_string(),
        }),
    }
}

async fn takeover(
    object_store: Arc<dyn ObjectStore>,
    data_path: &str,
    signals: &Path,
) -> Result<()> {
    wait_for_signal(signals, "incumbent-ready").await?;
    let shard = GraphShard::open_standalone_writer_with_options(
        data_path.to_string(),
        object_store,
        graph_options(),
    )
    .await?;
    shard.write_edge(mutation(100, 99, "takeover-99")).await?;
    signal(signals, "takeover-committed")?;
    wait_for_signal(signals, "stale-rejected").await?;
    shard.close().await?;
    println!("replacement writer committed and fenced the incumbent");
    Ok(())
}

async fn reader(object_store: Arc<dyn ObjectStore>, data_path: &str) -> Result<()> {
    let shard = GraphShard::open(data_path.to_string(), object_store).await?;
    let incumbent_visible = shard.edge_exists(CELL_ID, EDGE_TYPE, 100, 10).await?;
    let takeover_visible = shard.edge_exists(CELL_ID, EDGE_TYPE, 100, 99).await?;
    let stale_visible = shard.edge_exists(CELL_ID, EDGE_TYPE, 100, 777).await?;
    if !incumbent_visible || !takeover_visible || stale_visible {
        return Err(GraphError::CorruptValue {
            key: "fence/reader".to_string(),
            reason: format!(
                "unexpected visibility incumbent={incumbent_visible} takeover={takeover_visible} stale={stale_visible}"
            ),
        });
    }
    shard.close().await?;
    println!("reader verified durable takeover state and no stale write");
    Ok(())
}

fn mutation(src: u64, dst: u64, idempotency_key: &str) -> EdgeMutation {
    EdgeMutation {
        cell_id: CELL_ID.to_string(),
        edge_type: EDGE_TYPE.to_string(),
        src,
        dst,
        idempotency_key: idempotency_key.to_string(),
    }
}

fn signal(directory: &Path, name: &str) -> Result<()> {
    std::fs::write(directory.join(name), b"ready").map_err(io_error)
}

async fn wait_for_signal(directory: &Path, name: &str) -> Result<()> {
    let timeout = Duration::from_secs(env_u64("GRAPH_WORKER_TIMEOUT", 240));
    let started = Instant::now();
    let path = directory.join(name);
    while !path.exists() {
        if started.elapsed() >= timeout {
            return Err(GraphError::CorruptValue {
                key: path.display().to_string(),
                reason: "timed out waiting for fence worker signal".to_string(),
            });
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Ok(())
}

fn load_object_store(value: &str) -> Result<Arc<dyn ObjectStore>> {
    if let Some(path) = value.strip_prefix("local:") {
        local_object_store(path)
    } else if value == "-" {
        object_store_from_env(None)
    } else {
        object_store_from_env(Some(value.to_string()))
    }
}

fn graph_options() -> GraphOpenOptions {
    let mut options = GraphOpenOptions::default();
    options.index_policy = GraphIndexPolicy::OutboundOnly;
    options
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn io_error(error: std::io::Error) -> GraphError {
    GraphError::CorruptValue {
        key: "fence/signal".to_string(),
        reason: error.to_string(),
    }
}
