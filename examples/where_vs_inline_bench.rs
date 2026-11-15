//! Measures the cost of the two Cypher shapes that mean the same thing:
//!
//!   A  MATCH (s:Source {app_external_id: 'x'}) RETURN ...   -- inline property
//!   B  MATCH (s:Source) WHERE s.app_external_id = 'x' ...    -- WHERE predicate
//!
//! `best_row_node_access` (src/shard/query_optimizer.rs) picks a vertex access
//! path from `RowNodePattern.labels` and `RowNodePattern.properties` only. A
//! WHERE predicate lives in `RowMatchGroup.predicate` and never reaches that
//! function, so shape B plans `VertexLabelScan` over every `:Source` vertex,
//! hydrates each one, and applies the predicate afterwards. Shape A plans
//! `VertexPropertyIndex` and touches one key range.
//!
//! The harness seeds N `:Source` vertices with a unique `app_external_id`,
//! prints the plan each shape produces, then times both interleaved so
//! background drift hits them equally.
//!
//! Usage:
//!   cargo run --release --features opencypher --example where_vs_inline_bench \
//!     [VERTICES [ITERS [ENV_FILE]]]
//!
//! Defaults: 13008 vertices (a realistic :Source cardinality), 20
//! iterations, in-memory object store. Pass an env file (`AWS_ENDPOINT`,
//! `AWS_BUCKET`, ... — the format `scripts/minio_smoke.sh` writes) as the third
//! argument to run against a real object store, where the label scan also pays
//! per-read network latency and the gap widens.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hydradb::{object_store_from_env, GraphLimits, GraphShard, QueryContext, Result, RowQueryPlan, VertexMetadata, VertexPropertyValue};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

const CELL: &str = "bench-cell";
const LABEL: &str = "Source";
const PROPERTY: &str = "app_external_id";
/// Written in chunks so one transaction never carries the whole seed.
const SEED_CHUNK: usize = 2_000;

fn arg(index: usize, default: usize) -> usize {
    std::env::args().nth(index).and_then(|value| value.parse().ok()).unwrap_or(default)
}

/// The value the benchmark looks up: a vertex in the middle of the id space,
/// so a label scan cannot get lucky and stop early.
fn probe_value(vertices: usize) -> String {
    format!("ext-{:08}", vertices / 2)
}

#[tokio::main]
async fn main() -> Result<()> {
    let vertices = arg(1, 13_008);
    let iters = arg(2, 20);

    let store: Arc<dyn ObjectStore> = match std::env::args().nth(3) {
        Some(env_file) => {
            println!("object store from {env_file}");
            object_store_from_env(Some(env_file))?
        }
        None => {
            println!("object store: in-memory");
            Arc::new(InMemory::new())
        }
    };
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_millis();
    let shard =
        GraphShard::open_standalone_writer_with_limits(format!("bench/where-vs-inline-{run_id}").as_str(), store, GraphLimits::default())
            .await?;

    println!("seeding {vertices} :{LABEL} vertices with a unique {PROPERTY} ...");
    let started = Instant::now();
    for chunk_start in (0..vertices).step_by(SEED_CHUNK) {
        let chunk_end = (chunk_start + SEED_CHUNK).min(vertices);
        shard
            .set_vertex_metadata_batch(
                CELL,
                (chunk_start..chunk_end).map(|index| {
                    let mut labels = BTreeSet::new();
                    labels.insert(LABEL.to_string());
                    let mut properties = BTreeMap::new();
                    properties.insert(PROPERTY.to_string(), VertexPropertyValue::String(format!("ext-{index:08}")));
                    // A second property so hydrating a scanned vertex costs
                    // something, as it does on a real Source node.
                    properties.insert("name".to_string(), VertexPropertyValue::String(format!("source-{index}")));
                    (index as u64, VertexMetadata { labels, properties })
                }),
            )
            .await?;
    }
    println!("seeded in {} ms", started.elapsed().as_millis());

    let value = probe_value(vertices);
    let shapes = [
        (
            "A inline-property",
            format!("MATCH (s:{LABEL} {{{PROPERTY}: '{value}'}}) RETURN s.name AS name"),
        ),
        (
            "B where-predicate",
            format!("MATCH (s:{LABEL}) WHERE s.{PROPERTY} = '{value}' RETURN s.name AS name"),
        ),
        // STARTS WITH with the whole value matches the same single vertex, and
        // `prefix_predicate_seed_rows` (src/shard/query.rs) already seeds it
        // from the property index. Timing it shows what an equality pushdown
        // would buy, using machinery that is already in the tree.
        (
            "C starts-with",
            format!("MATCH (s:{LABEL}) WHERE s.{PROPERTY} STARTS WITH '{value}' RETURN s.name AS name"),
        ),
    ];
    // `MATCH (s) WHERE s.p = v` is *planned* as AllVertexScan but rejected at
    // execution with "node-only MATCH requires an id, label, or property
    // predicate" — the WHERE is invisible to that check too, so it is not
    // timed here.

    for (name, query) in &shapes {
        let plan = shard
            .explain_opencypher_rows(QueryContext::new(CELL, format!("explain-{name}")), query)
            .await?;
        println!("{name}\n  {query}\n{}", render_plan(&plan));
    }

    // One untimed pass each: fill the parsed-query cache and the SlateDB block
    // cache so the timed runs measure steady state, not first-touch.
    let mut expected = None::<usize>;
    for (name, query) in &shapes {
        let rows = shard
            .execute_cypher_rows(QueryContext::new(CELL, format!("warm-{name}")), query)
            .await?;
        match expected {
            None => expected = Some(rows.rows.len()),
            Some(count) => assert_eq!(count, rows.rows.len(), "{name} disagrees with the other shape on row count"),
        }
    }
    println!(
        "both shapes return {} row(s); timing {iters} interleaved iterations\n",
        expected.unwrap_or(0)
    );

    let mut samples: Vec<Vec<Duration>> = vec![Vec::with_capacity(iters); shapes.len()];
    for iteration in 0..iters {
        for (index, (name, query)) in shapes.iter().enumerate() {
            let started = Instant::now();
            let rows = shard
                .execute_cypher_rows(QueryContext::new(CELL, format!("bench-{name}-{iteration}")), query)
                .await?;
            let elapsed = started.elapsed();
            std::hint::black_box(&rows);
            samples[index].push(elapsed);
        }
    }

    println!(
        "{:<20} {:>10} {:>10} {:>10} {:>10}",
        "shape", "min_ms", "p50_ms", "mean_ms", "max_ms"
    );
    let mut medians = Vec::with_capacity(shapes.len());
    for (index, (name, _)) in shapes.iter().enumerate() {
        let mut sorted = samples[index].clone();
        sorted.sort();
        let median = sorted[sorted.len() / 2];
        medians.push(median);
        let mean = sorted.iter().sum::<Duration>() / sorted.len() as u32;
        println!(
            "{:<20} {:>10.3} {:>10.3} {:>10.3} {:>10.3}",
            name,
            ms(sorted[0]),
            ms(median),
            ms(mean),
            ms(sorted[sorted.len() - 1])
        );
    }
    let baseline = medians[0];
    println!("\nrelative to the inline-property plan, {vertices} :{LABEL} vertices:");
    for (index, (name, _)) in shapes.iter().enumerate().skip(1) {
        println!(
            "  {name:<20} {:.1}x  (+{:.3} ms per query)",
            ms(medians[index]) / ms(baseline).max(f64::MIN_POSITIVE),
            ms(medians[index]) - ms(baseline)
        );
    }

    // Every WHERE shape a pushdown pass would have to classify. Anything that
    // still prints `VertexLabelScan` is a shape the current planner cannot
    // anchor; anything that prints `VertexPropertyIndex` already works.
    println!("\n== plans for related WHERE shapes ==");
    let variants = [
        ("equality", format!("MATCH (s:{LABEL}) WHERE s.{PROPERTY} = '{value}' RETURN s.name AS name")),
        ("equality reversed", format!("MATCH (s:{LABEL}) WHERE '{value}' = s.{PROPERTY} RETURN s.name AS name")),
        ("equality AND other", format!("MATCH (s:{LABEL}) WHERE s.{PROPERTY} = '{value}' AND s.name <> 'zz' RETURN s.name AS name")),
        ("equality OR other", format!("MATCH (s:{LABEL}) WHERE s.{PROPERTY} = '{value}' OR s.name = 'zz' RETURN s.name AS name")),
        ("IN-style OR on one property", format!("MATCH (s:{LABEL}) WHERE s.{PROPERTY} = '{value}' OR s.{PROPERTY} = 'ext-00000001' RETURN s.name AS name")),
        ("NOT equality", format!("MATCH (s:{LABEL}) WHERE NOT s.{PROPERTY} = '{value}' RETURN s.name AS name")),
        ("STARTS WITH", format!("MATCH (s:{LABEL}) WHERE s.{PROPERTY} STARTS WITH '{value}' RETURN s.name AS name")),
        ("range", format!("MATCH (s:{LABEL}) WHERE s.{PROPERTY} > '{value}' RETURN s.name AS name")),
        ("optional match + equality", format!("MATCH (a:{LABEL} {{{PROPERTY}: '{value}'}}) OPTIONAL MATCH (s:{LABEL}) WHERE s.{PROPERTY} = '{value}' RETURN s.name AS name")),
        ("no label, equality only", format!("MATCH (s) WHERE s.{PROPERTY} = '{value}' RETURN s.name AS name")),
        ("inline property (control)", format!("MATCH (s:{LABEL} {{{PROPERTY}: '{value}'}}) RETURN s.name AS name")),
    ];
    for (name, query) in &variants {
        match shard
            .explain_opencypher_rows(QueryContext::new(CELL, format!("variant-{name}")), query)
            .await
        {
            Ok(plan) => {
                let accesses: Vec<String> = plan
                    .groups
                    .iter()
                    .flat_map(|group| group.patterns.iter())
                    .map(|pattern| format!("{:?} est={}", pattern.access, pattern.estimated_cardinality))
                    .collect();
                println!("  {name:<30} {}", accesses.join(" | "));
            }
            Err(err) => println!("  {name:<30} unsupported: {err}"),
        }
    }

    shard.close().await?;
    Ok(())
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn render_plan(plan: &RowQueryPlan) -> String {
    let mut out = String::new();
    for group in &plan.groups {
        out.push_str(&format!(
            "  group optional={} has_predicate={} est={} passes={:?}\n",
            group.optional, group.has_predicate, group.estimated_cardinality, group.optimizer_passes
        ));
        for pattern in &group.patterns {
            out.push_str(&format!(
                "    access={:?} est={} bindings={:?} passes={:?}\n",
                pattern.access, pattern.estimated_cardinality, pattern.bindings, pattern.optimizer_passes
            ));
        }
    }
    out
}
