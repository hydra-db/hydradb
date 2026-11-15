use hydradb::{
    local_object_store, CypherEngineMode, GraphLimits, GraphOpenOptions, GraphReaderMode, GraphShard, QueryContext, VertexPropertyValue,
};
use sha2::{Digest, Sha256};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args.next().expect("local store path");
    let statement = std::fs::read_to_string(args.next().expect("query file"))?;
    let limit = args.next().map(|n| n.parse::<u64>()).transpose()?.unwrap_or(20001);
    let both = args.next().as_deref() == Some("both");
    let experimental_first = args.next().as_deref() == Some("experimental-first");
    let options = GraphOpenOptions::default();
    let options = GraphOpenOptions {
        reader_mode: GraphReaderMode::FollowLatest,
        limits: GraphLimits {
            max_query_runtime_ms: Some(120_000),
            ..GraphLimits::default()
        },
        ..options
    };
    let shard = GraphShard::open_with_options("graph", local_object_store(root)?, options).await?;
    let modes = if both {
        if experimental_first {
            vec![
                CypherEngineMode::Experimental,
                CypherEngineMode::Legacy,
                CypherEngineMode::Legacy,
                CypherEngineMode::Experimental,
            ]
        } else {
            vec![
                CypherEngineMode::Legacy,
                CypherEngineMode::Experimental,
                CypherEngineMode::Experimental,
                CypherEngineMode::Legacy,
            ]
        }
    } else {
        vec![CypherEngineMode::Experimental]
    };
    let mut previous = None;
    for mode in modes {
        let before_operators = shard.graph_operational_metrics().experimental_operators;
        let started = std::time::Instant::now();
        let result = shard
            .execute_cypher_rows(
                QueryContext::new("cell-0", "ordered-relationship-fetch-bench")
                    .with_parameter("limit", VertexPropertyValue::Integer(limit))
                    .with_cypher_engine(mode),
                &statement,
            )
            .await;
        let query_ms = started.elapsed().as_secs_f64() * 1000.0;
        match result {
            Ok(result) => {
                for operator in shard.graph_operational_metrics().experimental_operators {
                    let before = before_operators.iter().find(|old| old.operator == operator.operator);
                    let counters = operator
                        .counter_fields()
                        .into_iter()
                        .map(|(name, value)| {
                            let previous = before
                                .and_then(|old| {
                                    old.counter_fields()
                                        .into_iter()
                                        .find(|(key, _)| *key == name)
                                        .map(|(_, value)| value)
                                })
                                .unwrap_or(0);
                            (name, value.saturating_sub(previous))
                        })
                        .collect::<std::collections::BTreeMap<_, _>>();
                    println!(
                        "FETCH_BENCH_OPERATOR engine={mode:?} operator={} counters={}",
                        operator.operator,
                        serde_json::to_string(&counters)?
                    );
                }
                let hash_started = std::time::Instant::now();
                let mut digest = Sha256::new();
                for row in &result.rows {
                    let encoded = format!("{:?}", row.values);
                    digest.update((encoded.len() as u64).to_be_bytes());
                    digest.update(encoded.as_bytes());
                }
                let row_hash = format!("{:x}", digest.finalize());
                let hash_ms = hash_started.elapsed().as_secs_f64() * 1000.0;
                println!(
                    "FETCH_BENCH_QUERY_COMPLETED engine={mode:?} rows={} native_debug_sha256={} query_ms={} hash_ms={} elapsed_ms={}",
                    result.rows.len(),
                    row_hash,
                    query_ms,
                    hash_ms,
                    started.elapsed().as_secs_f64() * 1000.0
                );
                if let Some(previous) = &previous {
                    let equal = previous == &result.rows;
                    println!("FETCH_BENCH_ORDERED_ROW_PARITY {equal}");
                    if !equal {
                        return Err("Ordered result parity failed".into());
                    }
                }
                previous = Some(result.rows);
            }
            Err(error) => return Err(format!("FETCH_BENCH_QUERY_FAILED engine={mode:?} query_ms={query_ms} {error}").into()),
        }
    }
    shard.close().await?;
    Ok(())
}
