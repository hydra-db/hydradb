//! Per-query timing runner. Produces a `QueryResult` per (query, parameter)
//! tuple with cold/warm timings and the row count turbolay returned. Output
//! JSON (`BenchOutput`) is shape-compatible with the NamiDB reference
//! harness's own `runner.rs` so `bench/py/compare.py` can diff the two.

use std::time::Instant;

use anyhow::Result;
use serde::Serialize;
use turbolay::write::Writer;

use crate::dataset::DatasetSizes;
use crate::queries::{self, Query, Schema};

/// One bench iteration produces this record. Times are in microseconds.
#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    pub backend: String,
    pub query: &'static str,
    /// KNOWS-prefix hop depth this row was run at (the bench hop-sweep axis).
    pub hops: usize,
    pub param: String,
    pub rows: usize,
    pub cold_us: u64,
    pub warm_p50_us: u64,
    pub warm_p95_us: u64,
    pub warm_p99_us: u64,
    pub warm_runs: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchOutput {
    pub scale: f64,
    pub seed: u64,
    /// `memory://bench`, `local://<path>`, or `s3://<bucket>` — see
    /// `main.rs`'s `backend_label`.
    pub backend: String,
    /// Target hub degree the dataset was generated with (0 = no hubs). The
    /// supernode-degree axis of the bench sweep; captured here so each output
    /// file self-describes which degree tier it belongs to.
    pub hub_degree: usize,
    pub dataset_sizes: SizesReport,
    pub results: Vec<QueryResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SizesReport {
    pub persons: usize,
    pub posts: usize,
    pub comments: usize,
    pub knows: usize,
    pub has_creator: usize,
    pub likes: usize,
    pub reply_of: usize,
}

impl From<&DatasetSizes> for SizesReport {
    fn from(s: &DatasetSizes) -> Self {
        Self {
            persons: s.persons,
            posts: s.posts,
            comments: s.comments,
            knows: s.knows,
            has_creator: s.has_creator,
            likes: s.likes,
            reply_of: s.reply_of,
        }
    }
}

/// Runs `query` once (timed as `cold`) then `warm_runs` more times against
/// the same live `Writer`. M1 turbolay has no snapshot/cache layer to warm up
/// yet, so "cold" vs "warm" here mostly captures run-to-run jitter rather
/// than a real cache-cold/cache-hot distinction — kept for JSON-shape parity
/// with the NamiDB reference harness (and as a seam for when turbolay grows
/// its own read-side caches).
pub async fn run_query(
    writer: &Writer,
    schema: &Schema,
    backend: &str,
    query: Query,
    param: &str,
    warm_runs: usize,
    hops: Option<usize>,
) -> Result<QueryResult> {
    let cold_start = Instant::now();
    let cold_rows = queries::execute_with_hops(writer, schema, query, param, hops).await?;
    let cold_us = cold_start.elapsed().as_micros() as u64;

    let mut times: Vec<u64> = Vec::with_capacity(warm_runs);
    for _ in 0..warm_runs {
        let start = Instant::now();
        let _ = queries::execute_with_hops(writer, schema, query, param, hops).await?;
        times.push(start.elapsed().as_micros() as u64);
    }
    times.sort_unstable();

    // Effective hop depth actually run (for the JSON matrix): the override if
    // present, else the query's natural depth (shared source of truth so `run`
    // and `verify` label cells identically).
    let effective_hops = hops.unwrap_or_else(|| query.natural_hops());

    Ok(QueryResult {
        backend: backend.to_string(),
        query: query.name(),
        hops: effective_hops,
        param: param.to_string(),
        rows: cold_rows.len(),
        cold_us,
        warm_p50_us: pct(&times, 0.50),
        warm_p95_us: pct(&times, 0.95),
        warm_p99_us: pct(&times, 0.99),
        warm_runs,
    })
}

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{self, DatasetConfig};
    use crate::loader;

    /// Exact copy of `main.rs`'s `make_person_id_hex` (kept private per file
    /// so the test doesn't need `main.rs`'s CLI plumbing in scope).
    fn make_person_id_hex(i: usize) -> String {
        let mut bytes = [0u8; 16];
        bytes[0] = b'P';
        let i_bytes = (i as u128).to_be_bytes();
        bytes[1..].copy_from_slice(&i_bytes[1..]);
        let mut s = String::with_capacity(32);
        for b in bytes {
            let _ = std::fmt::Write::write_fmt(&mut s, format_args!("{:02x}", b));
        }
        s
    }

    #[tokio::test]
    async fn smoke_generate_load_and_run_three_queries() {
        let tmp = std::env::temp_dir().join(format!(
            "turbolay-bench-smoke-{}",
            uuid::Uuid::now_v7().simple()
        ));
        std::fs::create_dir_all(&tmp).unwrap();

        let sizes = dataset::generate(
            &tmp,
            &DatasetConfig {
                scale: 0.01,
                seed: 7,
                hub_count: 0,
                hub_degree: 0,
            },
        )
        .unwrap();
        assert!(sizes.persons >= 10);

        let mut writer = Writer::in_memory().await.unwrap();
        // Legacy per-record path — this smoke test is about query
        // correctness, not ingest batching (that's `write.rs`'s own
        // `ingest_batch` correctness-oracle tests).
        loader::load_into_writer(&mut writer, &tmp, 0)
            .await
            .unwrap();
        let schema = Schema::resolve(&writer).unwrap();

        let param = make_person_id_hex(0);

        let ic02 = queries::execute(&writer, &schema, Query::Ic02, &param)
            .await
            .unwrap();
        let ic09 = queries::execute(&writer, &schema, Query::Ic09, &param)
            .await
            .unwrap();
        let ic3h = queries::execute(&writer, &schema, Query::Ic3h, &param)
            .await
            .unwrap();

        // No hard cardinality assertion: scale=0.01 is a tiny, uniformly
        // random graph, so "person 0 has any 2/3-hop KNOWS friends with
        // posts" isn't guaranteed. "Executes without error" plus the ORDER BY
        // check below (on whichever query does have rows) is the invariant
        // this smoke test owns.
        let _ = (ic09.len(), ic3h.len());

        // ORDER BY messageCreationDate DESC: column index 3 is creationDate.
        let dates: Vec<i64> = ic02.iter().map(|row| row.0[3].as_i64().unwrap()).collect();
        let mut sorted_desc = dates.clone();
        sorted_desc.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(
            dates, sorted_desc,
            "ic02 rows must be sorted by creationDate DESC"
        );

        std::fs::remove_dir_all(&tmp).ok();
    }
}
