//! The entity-dedup sweep's keyset page, on a cell many tenants share.
//!
//! When every tenant runs in `cell-0`, the `created_at` index the
//! ordered window walks holds every tenant's entities. The sweep asks for
//! entities after its watermark; with nothing new, the right answer is empty
//! and should cost almost nothing. The experimental engine instead loaded
//! ~45k vertices of other tenants to reject them.
//!
//! The fixture varies where the other tenants' rows sit relative to the
//! cursor, because that decides which fix can help:
//!
//! - `newer`: every other tenant's rows sort after the cursor.
//! - `older`: every other tenant's rows sort before it.
//! - `mixed`: half each side.
//!
//! In every layout the quiet tenant has `tenant_id == sub_tenant_id` and more
//! rows than the experimental equality probe's allowance, which is the production
//! shape that makes the probe give up.

use std::sync::Arc;
use std::time::{Duration, Instant};

use slatedb::object_store::memory::InMemory;

use crate::{CypherEngineMode, GraphLimits, GraphShard, QueryContext, QueryValue, VertexMetadata, VertexPropertyValue};

const CELL: &str = "cell-a";
/// Above the experimental probe's per-candidate allowance with two
/// candidates (4,096 / 2), far below legacy's (250,000 / 2).
const QUIET_ROWS: u64 = 3_000;
const NARROW_ROWS: u64 = 8;
const OTHER_ROWS: u64 = 5_000;
/// Ingestion stamps one `created_at` per batch, so ties are the norm.
const BATCH: u64 = 50;
const LIMIT: usize = 250;

#[derive(Clone, Copy, Debug)]
enum Layout {
    Newer,
    Older,
    Mixed,
}

fn entity(tenant: &str, sub_tenant: &str, created_at: String, entity_id: String) -> VertexMetadata {
    VertexMetadata::default()
        .with_label("Entity")
        .with_property("tenant_id", VertexPropertyValue::String(tenant.to_string()))
        .with_property("sub_tenant_id", VertexPropertyValue::String(sub_tenant.to_string()))
        .with_property("created_at", VertexPropertyValue::String(created_at))
        .with_property("entity_id", VertexPropertyValue::String(entity_id))
        .with_property("name", VertexPropertyValue::String("n".to_string()))
        .with_property("type", VertexPropertyValue::String("t".to_string()))
        .with_property("namespace", VertexPropertyValue::String("ns".to_string()))
}

/// `2026-03-15T00:00:00.000123` style: sortable, one value per batch.
fn stamp(month: &str, batch: u64) -> String {
    format!("2026-{month}T00:00:00.{batch:06}")
}

struct Fixture {
    shard: GraphShard,
    /// (created_at, entity_id) of the quiet sub-tenant's newest row.
    quiet_cursor: (String, String),
    narrow_cursor: (String, String),
}

async fn fixture(layout: Layout) -> Fixture {
    let shard = GraphShard::open_standalone_writer_with_limits(
        format!("graph/keyset-shared-cell-{layout:?}"),
        Arc::new(InMemory::new()),
        GraphLimits::default(),
    )
    .await
    .expect("open graph shard");

    // The quiet tenant writes in March. Its narrow sub-tenant shares the
    // tenant_id and writes in the same month.
    let mut rows = Vec::new();
    for i in 0..QUIET_ROWS {
        rows.push((1 + i, entity("q", "q", stamp("03-15", i / BATCH), format!("q-{i:05}"))));
    }
    for i in 0..NARROW_ROWS {
        rows.push((100_000 + i, entity("q", "narrow", stamp("03-15", i), format!("n-{i:05}"))));
    }
    for i in 0..OTHER_ROWS {
        let month = match (layout, i % 2) {
            (Layout::Newer, _) | (Layout::Mixed, 0) => "06-15",
            (Layout::Older, _) | (Layout::Mixed, _) => "01-15",
        };
        let tenant = format!("o{}", i % 20);
        rows.push((200_000 + i, entity(&tenant, &tenant, stamp(month, i / BATCH), format!("o-{i:05}"))));
    }
    shard.set_vertex_metadata_batch(CELL, rows).await.expect("seed the shared cell");

    let last_quiet = QUIET_ROWS - 1;
    let last_narrow = NARROW_ROWS - 1;
    Fixture {
        shard,
        quiet_cursor: (stamp("03-15", last_quiet / BATCH), format!("q-{last_quiet:05}")),
        narrow_cursor: (stamp("03-15", last_narrow), format!("n-{last_narrow:05}")),
    }
}

/// The exact shape of a `pendingEntityQuery` client query, with the
/// parameters inlined because this entry point takes no parameter map.
fn pending_query(tenant: &str, sub_tenant: &str, cursor: &(String, String)) -> String {
    let (created_at, entity_id) = cursor;
    format!(
        "MATCH (e:Entity {{tenant_id: '{tenant}', sub_tenant_id: '{sub_tenant}'}}) \
         WHERE (e.created_at > '{created_at}' \
            OR (e.created_at = '{created_at}' AND e.entity_id > '{entity_id}')) \
            AND e.created_at STARTS WITH '' \
         RETURN e.entity_id AS entity_id, e.name AS name, e.type AS type, \
                e.namespace AS namespace, e.created_at AS created_at \
         ORDER BY e.created_at, e.entity_id \
         LIMIT {LIMIT}"
    )
}

#[derive(Debug, Default, Clone, Copy)]
struct Cost {
    hydrated: u64,
    storage_requests: u64,
    elapsed: Duration,
}

fn experimental_totals(shard: &GraphShard) -> (u64, u64) {
    shard
        .graph_operational_metrics()
        .experimental_operators
        .iter()
        .fold((0, 0), |(hydrated, requests), operator| {
            (hydrated + operator.hydrated_vertices, requests + operator.storage_requests)
        })
}

async fn run(shard: &GraphShard, engine: CypherEngineMode, query: &str) -> (Vec<String>, Cost) {
    let before = experimental_totals(shard);
    let started = Instant::now();
    let result = shard
        .execute_cypher_rows(QueryContext::new(CELL, "keyset-shared-cell").with_cypher_engine(engine), query)
        .await
        .unwrap_or_else(|error| panic!("{engine:?}: {query}: {error}"));
    let elapsed = started.elapsed();
    let after = experimental_totals(shard);
    let ids = result
        .rows
        .iter()
        .map(|row| match &row.values[0] {
            QueryValue::Property(VertexPropertyValue::String(id)) => id.clone(),
            other => panic!("unexpected entity_id: {other:?}"),
        })
        .collect();
    (
        ids,
        Cost {
            hydrated: after.0 - before.0,
            storage_requests: after.1 - before.1,
            elapsed,
        },
    )
}

/// Loaded vertices a correct empty page may cost: one window's worth, plus a
/// seed small enough to be loaded whole.
const HYDRATION_BUDGET: u64 = LIMIT as u64 + 2 * NARROW_ROWS;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyset_last_page_on_a_shared_cell_is_bounded() {
    let mut report = Vec::new();
    let mut over_budget = Vec::new();
    for layout in [Layout::Newer, Layout::Older, Layout::Mixed] {
        let fixture = fixture(layout).await;
        let cases = [
            ("wide", pending_query("q", "q", &fixture.quiet_cursor)),
            ("narrow", pending_query("q", "narrow", &fixture.narrow_cursor)),
        ];
        for (case, query) in &cases {
            let (legacy_rows, legacy) = run(&fixture.shard, CypherEngineMode::Legacy, query).await;
            let (rows, cost) = run(&fixture.shard, CypherEngineMode::Experimental, query).await;
            assert_eq!(rows, legacy_rows, "{layout:?}/{case}: engines disagree");
            assert!(rows.is_empty(), "{layout:?}/{case}: nothing is newer: {rows:?}");
            report.push(format!(
                "{:<6} {:<6} exp hydrated={:>5} storage_req={:>6} {:>8.1?} | legacy {:>8.1?}",
                format!("{layout:?}"),
                case,
                cost.hydrated,
                cost.storage_requests,
                cost.elapsed,
                legacy.elapsed,
            ));
            if cost.hydrated > HYDRATION_BUDGET {
                over_budget.push(format!("{layout:?}/{case}: {}", cost.hydrated));
            }
        }

        // New rows after the cursor, newer than every other tenant's: the
        // walk has to get past all of them to find these.
        let fresh = (0..5)
            .map(|i| (300_000 + i, entity("q", "q", stamp("12-15", 0), format!("q-new-{i}"))))
            .collect::<Vec<_>>();
        fixture
            .shard
            .set_vertex_metadata_batch(CELL, fresh)
            .await
            .expect("write fresh rows");
        let query = &cases[0].1;
        let (legacy_rows, _) = run(&fixture.shard, CypherEngineMode::Legacy, query).await;
        let (rows, cost) = run(&fixture.shard, CypherEngineMode::Experimental, query).await;
        let expected = (0..5).map(|i| format!("q-new-{i}")).collect::<Vec<_>>();
        assert_eq!(legacy_rows, expected, "{layout:?}/fresh: legacy");
        assert_eq!(rows, expected, "{layout:?}/fresh: experimental");
        report.push(format!(
            "{:<6} {:<6} exp hydrated={:>5} storage_req={:>6} {:>8.1?}",
            format!("{layout:?}"),
            "fresh",
            cost.hydrated,
            cost.storage_requests,
            cost.elapsed,
        ));
        fixture.shard.close().await.expect("close graph shard");
    }
    eprintln!("\n{}", report.join("\n"));
    assert!(
        over_budget.is_empty(),
        "empty keyset pages loaded more than {HYDRATION_BUDGET} vertices: {over_budget:?}"
    );
}
