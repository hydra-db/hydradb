#[cfg(feature = "experimental-cypher-engine")]
use std::{io, path::Path, path::PathBuf, time::Instant};

#[cfg(feature = "experimental-cypher-engine")]
use hydradb::{
    local_object_store, CypherEngineMode, GraphLimits, GraphShard, QueryContext, QueryValue, VertexMetadata, VertexPropertyValue,
};

#[cfg(not(feature = "experimental-cypher-engine"))]
fn main() {
    eprintln!("enable --features experimental-cypher-engine");
    std::process::exit(2);
}

#[cfg(feature = "experimental-cypher-engine")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_default();
    let store = args.next().map(PathBuf::from).unwrap_or_default();
    let tie_rows = parse_usize(args.next(), "tie_rows")?;
    let query_limit = args
        .next()
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?
        .unwrap_or(10);
    if mode.is_empty() || store.as_os_str().is_empty() || tie_rows == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: ordered_tie_memory_probe <seed|query> <store> <tie_rows> [query_limit]".to_string(),
        )
        .into());
    }

    match mode.as_str() {
        "seed" => seed(&store, tie_rows).await,
        "query" => query(&store, tie_rows, query_limit).await,
        other => Err(io::Error::new(io::ErrorKind::InvalidInput, format!("unknown mode {other}; expected seed or query")).into()),
    }
}

#[cfg(feature = "experimental-cypher-engine")]
fn parse_usize(value: Option<String>, field: &str) -> Result<usize, io::Error> {
    value
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("missing {field}")))?
        .parse::<usize>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, format!("invalid {field}: {error}")))
}

#[cfg(feature = "experimental-cypher-engine")]
async fn seed(store: &Path, tie_rows: usize) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(store)?;
    let started = Instant::now();
    let shard = GraphShard::open_standalone_writer("ordered-tie-memory", local_object_store(store)?).await?;
    for start in (1..=tie_rows).step_by(10_000) {
        let end = tie_rows.min(start.saturating_add(9_999));
        shard
            .set_vertex_metadata_batch(
                "cell-a",
                (start..=end).map(|vertex_id| {
                    (
                        vertex_id as u64,
                        VertexMetadata::default()
                            .with_label("Entity")
                            .with_property("created_at", VertexPropertyValue::String("2026-01-01".to_string())),
                    )
                }),
            )
            .await?;
    }
    shard.close().await?;
    println!(
        "{{\"phase\":\"seed\",\"tie_rows\":{tie_rows},\"elapsed_ms\":{}}}",
        started.elapsed().as_secs_f64() * 1_000.0
    );
    Ok(())
}

#[cfg(feature = "experimental-cypher-engine")]
async fn query(store: &Path, tie_rows: usize, query_limit: usize) -> Result<(), Box<dyn std::error::Error>> {
    let work_limit = tie_rows.saturating_add(1);
    let limits = GraphLimits {
        max_query_intermediate_rows: work_limit,
        max_query_index_candidates: work_limit,
        max_query_runtime_ms: None,
        ..GraphLimits::default()
    };
    let shard = GraphShard::open_standalone_writer_with_limits("ordered-tie-memory", local_object_store(store)?, limits).await?;
    let statement = format!(
        "MATCH (n:Entity) WHERE n.created_at STARTS WITH '' \
         RETURN n.id AS id ORDER BY n.created_at ASC, n.id DESC LIMIT {query_limit}"
    );
    let started = Instant::now();
    let result = shard
        .execute_cypher_rows(
            QueryContext::new("cell-a", "ordered-tie-memory-probe").with_cypher_engine(CypherEngineMode::Experimental),
            &statement,
        )
        .await?;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let actual = result
        .rows
        .iter()
        .map(|row| match row.values[0] {
            QueryValue::VertexId(id) => id,
            ref other => panic!("unexpected ID: {other:?}"),
        })
        .collect::<Vec<_>>();
    let expected = (1..=tie_rows as u64).rev().take(query_limit).collect::<Vec<_>>();
    assert_eq!(actual, expected, "exact ordered winners");
    let metrics = shard.graph_operational_metrics();
    shard.close().await?;
    println!(
        "{{\"phase\":\"query\",\"tie_rows\":{tie_rows},\"query_limit\":{query_limit},\"returned_rows\":{},\"elapsed_ms\":{elapsed_ms},\"ordered_scan_requests\":{}}}",
        result.rows.len(),
        metrics.query_experimental_ordered_property_scan_requests
    );
    Ok(())
}
