use std::path::PathBuf;
use std::time::Instant;

use falkor_dump_import::{
    build_snapshot_spill_with_source_layout, load_spilled_scope, DedicatedLayout, GraphSelection, SnapshotGraph, SourceLayout,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Serialize)]
struct ScopeSummary {
    scope_sha256: String,
    vertices: usize,
    relationships: usize,
    orphan_edges: usize,
    relationship_anomalies: usize,
    edge_types: usize,
    merged_vertices: u64,
    merged_relationships: u64,
}
#[derive(Debug, Serialize)]
struct PlanSummary {
    format: &'static str,
    snapshot_id: String,
    manifest_sha256: String,
    dump_objects: usize,
    dump_bytes: u64,
    selected_dump_objects: usize,
    selected_dump_bytes: u64,
    included_graphs: Vec<SnapshotGraph>,
    excluded_graphs: Vec<SnapshotGraph>,
    scopes: usize,
    vertices: u64,
    relationships: u64,
    orphan_edges: u64,
    relationship_anomalies: u64,
    quarantined_vertices: u64,
    workers: usize,
    elapsed_milliseconds: u128,
    results: Vec<ScopeSummary>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("falkor-dump-plan: {error}");
        std::process::exit(1);
    }
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let root = args
        .next()
        .map(PathBuf::from)
        .ok_or("usage: falkor-dump-plan SNAPSHOT_ROOT [WORKERS]")?;
    let mut workers = std::thread::available_parallelism().map_or(1, usize::from);
    let mut selection = GraphSelection::All;
    let mut dedicated_layout = None;
    let mut byog = false;
    let mut first = true;
    while let Some(argument) = args.next() {
        let argument = argument.into_string().map_err(|_| "graph selection must be UTF-8")?;
        if first && !argument.starts_with("--") {
            workers = argument.parse::<usize>()?.max(1);
        } else if argument == "--byog" {
            byog = true;
        } else if argument == "--dedicated-layout" {
            dedicated_layout = Some(DedicatedLayout::read(&PathBuf::from(
                args.next().ok_or("--dedicated-layout requires a path")?,
            ))?);
        } else if argument == "--include-graph" || argument == "--exclude-graph" {
            let graph = args
                .next()
                .ok_or_else(|| format!("{argument} requires a value"))?
                .into_string()
                .map_err(|_| "graph selection must be UTF-8")?;
            selection.add_graph(&argument, graph)?;
        } else {
            return Err("usage: falkor-dump-plan SNAPSHOT_ROOT [WORKERS] [--include-graph NAME ... | --exclude-graph NAME ...]".into());
        }
        first = false;
    }
    if byog && dedicated_layout.is_some() {
        return Err("--byog and --dedicated-layout are mutually exclusive".into());
    }
    let started = Instant::now();
    let scratch = tempfile::tempdir()?;
    let plan = build_snapshot_spill_with_source_layout(
        &root,
        workers,
        scratch.path(),
        &selection,
        if byog {
            SourceLayout::Byog
        } else {
            dedicated_layout.as_ref().map_or(SourceLayout::Shared, SourceLayout::Dedicated)
        },
    )?;
    let mut results = Vec::with_capacity(plan.scopes.len());
    for spilled in &plan.scopes {
        let scope = load_spilled_scope(spilled)?;
        let edge_types = scope
            .relationships
            .values()
            .map(|r| r.edge_type.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        results.push(ScopeSummary {
            scope_sha256: format!("{:x}", Sha256::digest(scope.scope.to_string().as_bytes())),
            vertices: scope.vertices.len(),
            relationships: scope.relationships.len(),
            orphan_edges: scope.orphan_edges.len(),
            relationship_anomalies: scope.relationship_anomalies.len(),
            edge_types,
            merged_vertices: scope.diagnostics.merged_vertices,
            merged_relationships: scope.diagnostics.merged_relationships,
        });
    }
    results.sort_by(|a, b| a.scope_sha256.cmp(&b.scope_sha256));
    let summary = PlanSummary {
        format: "falkor-dump-import-plan-v1",
        snapshot_id: plan.snapshot_id,
        manifest_sha256: plan.manifest_sha256,
        dump_objects: plan.object_count,
        dump_bytes: plan.dump_bytes,
        selected_dump_objects: plan.selected_object_count,
        selected_dump_bytes: plan.selected_dump_bytes,
        included_graphs: plan.included_graphs,
        excluded_graphs: plan.excluded_graphs,
        scopes: results.len(),
        vertices: results.iter().map(|s| s.vertices as u64).sum(),
        relationships: results.iter().map(|s| s.relationships as u64).sum(),
        orphan_edges: results.iter().map(|s| s.orphan_edges as u64).sum(),
        relationship_anomalies: results.iter().map(|s| s.relationship_anomalies as u64).sum(),
        quarantined_vertices: plan.unscoped_vertices.count,
        workers,
        elapsed_milliseconds: started.elapsed().as_millis(),
        results,
    };
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
