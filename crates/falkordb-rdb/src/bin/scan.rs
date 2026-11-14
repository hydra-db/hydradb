use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const SNAPSHOT_FORMAT: &str = "falkordb-redis-dump-per-graph-v1";

#[derive(Debug, Deserialize)]
struct Manifest {
    format: String,
    snapshot_id: String,
    dumped_graphs: usize,
    dump_bytes: u64,
    objects: Vec<ManifestObject>,
}

#[derive(Debug, Deserialize)]
struct ManifestObject {
    object: String,
    graph_utf8: String,
    bytes: u64,
    sha256: String,
    status: String,
}

#[derive(Debug, Serialize)]
struct ObjectSummary {
    object: String,
    nodes: usize,
    edge_records: usize,
    relationships: usize,
    relationships_with_missing_endpoints: u64,
    relationship_cells_without_records: u64,
    edge_records_without_cells: u64,
    reused_edge_id_cells: u64,
    relationship_pairs_without_adjacency: u64,
    adjacency_pairs_without_relationships: u64,
    dangling_tensor_cells: u64,
    empty_tensor_cells: u64,
    labeled_nodes: usize,
    adjacency_pairs: usize,
    elapsed_microseconds: u64,
}

#[derive(Debug, Serialize)]
struct ScanSummary {
    format: &'static str,
    snapshot_id: String,
    manifest_sha256: String,
    objects: usize,
    bytes: u64,
    nodes: u64,
    edge_records: u64,
    relationships: u64,
    relationships_with_missing_endpoints: u64,
    relationship_cells_without_records: u64,
    edge_records_without_cells: u64,
    reused_edge_id_cells: u64,
    relationship_pairs_without_adjacency: u64,
    adjacency_pairs_without_relationships: u64,
    dangling_tensor_cells: u64,
    empty_tensor_cells: u64,
    labeled_nodes: u64,
    adjacency_pairs: u64,
    workers: usize,
    elapsed_milliseconds: u128,
    objects_per_second: f64,
    mebibytes_per_second: f64,
    results: Vec<ObjectSummary>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("falkordb-rdb-scan: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let root = args
        .next()
        .map(PathBuf::from)
        .ok_or("usage: falkordb-rdb-scan SNAPSHOT_ROOT [WORKERS]")?;
    let workers = args
        .next()
        .map(|value| value.to_string_lossy().parse::<usize>())
        .transpose()?
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from))
        .max(1);
    if args.next().is_some() {
        return Err("usage: falkordb-rdb-scan SNAPSHOT_ROOT [WORKERS]".into());
    }
    let manifest_path = root.join("manifest.json");
    let manifest_bytes = std::fs::read(&manifest_path)?;
    let manifest_sha256 = format!("{:x}", Sha256::digest(&manifest_bytes));
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;
    validate_manifest(&manifest)?;

    let objects = Arc::new(manifest.objects);
    let next = Arc::new(AtomicUsize::new(0));
    let summaries = Arc::new(Mutex::new(Vec::with_capacity(objects.len())));
    let failures = Arc::new(Mutex::new(Vec::<String>::new()));
    let started = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..workers.min(objects.len().max(1)) {
            let root = &root;
            let objects = Arc::clone(&objects);
            let next = Arc::clone(&next);
            let summaries = Arc::clone(&summaries);
            let failures = Arc::clone(&failures);
            scope.spawn(move || loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                let Some(object) = objects.get(index) else {
                    break;
                };
                match scan_object(root, object) {
                    Ok(summary) => summaries.lock().expect("summary mutex poisoned").push(summary),
                    Err(error) => {
                        failures
                            .lock()
                            .expect("failure mutex poisoned")
                            .push(format!("{}: {error}", object.object));
                    }
                }
            });
        }
    });
    let mut failures = Arc::try_unwrap(failures)
        .map_err(|_| "scan failures still shared")?
        .into_inner()
        .map_err(|_| "failure mutex poisoned")?;
    if !failures.is_empty() {
        failures.sort();
        return Err(format!("{} object(s) failed validation:\n{}", failures.len(), failures.join("\n")).into());
    }
    let mut results = Arc::try_unwrap(summaries)
        .map_err(|_| "scan summary still shared")?
        .into_inner()
        .map_err(|_| "summary mutex poisoned")?;
    results.sort_by(|left, right| left.object.cmp(&right.object));
    if results.len() != objects.len() {
        return Err(format!("decoded {} objects, expected {}", results.len(), objects.len()).into());
    }
    let elapsed = started.elapsed();
    let summary = ScanSummary {
        format: "falkordb-rdb-scan-v1",
        snapshot_id: manifest.snapshot_id.clone(),
        manifest_sha256,
        objects: results.len(),
        bytes: manifest.dump_bytes,
        nodes: results.iter().map(|item| item.nodes as u64).sum(),
        edge_records: results.iter().map(|item| item.edge_records as u64).sum(),
        relationships: results.iter().map(|item| item.relationships as u64).sum(),
        relationships_with_missing_endpoints: results.iter().map(|item| item.relationships_with_missing_endpoints).sum(),
        relationship_cells_without_records: results.iter().map(|item| item.relationship_cells_without_records).sum(),
        edge_records_without_cells: results.iter().map(|item| item.edge_records_without_cells).sum(),
        reused_edge_id_cells: results.iter().map(|item| item.reused_edge_id_cells).sum(),
        relationship_pairs_without_adjacency: results.iter().map(|item| item.relationship_pairs_without_adjacency).sum(),
        adjacency_pairs_without_relationships: results.iter().map(|item| item.adjacency_pairs_without_relationships).sum(),
        dangling_tensor_cells: results.iter().map(|item| item.dangling_tensor_cells).sum(),
        empty_tensor_cells: results.iter().map(|item| item.empty_tensor_cells).sum(),
        labeled_nodes: results.iter().map(|item| item.labeled_nodes as u64).sum(),
        adjacency_pairs: results.iter().map(|item| item.adjacency_pairs as u64).sum(),
        workers,
        elapsed_milliseconds: elapsed.as_millis(),
        objects_per_second: results.len() as f64 / elapsed.as_secs_f64(),
        mebibytes_per_second: manifest.dump_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
        results,
    };
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

fn validate_manifest(manifest: &Manifest) -> Result<(), Box<dyn std::error::Error>> {
    if manifest.format != SNAPSHOT_FORMAT {
        return Err(format!("snapshot format is {}, expected {SNAPSHOT_FORMAT}", manifest.format).into());
    }
    if manifest.dumped_graphs != manifest.objects.len() {
        return Err(format!(
            "manifest says {} graphs but lists {} objects",
            manifest.dumped_graphs,
            manifest.objects.len()
        )
        .into());
    }
    let listed_bytes: u64 = manifest.objects.iter().map(|object| object.bytes).sum();
    if listed_bytes != manifest.dump_bytes {
        return Err(format!("manifest says {} bytes but object list totals {listed_bytes}", manifest.dump_bytes).into());
    }
    if let Some(object) = manifest.objects.iter().find(|object| object.status != "dumped") {
        return Err(format!("{} has status {}", object.object, object.status).into());
    }
    let mut paths = BTreeSet::new();
    for object in &manifest.objects {
        let path = Path::new(&object.object);
        if path.as_os_str().is_empty() || path.components().any(|component| !matches!(component, Component::Normal(_))) {
            return Err(format!("object path {:?} is not a safe relative path", object.object).into());
        }
        if !paths.insert(object.object.as_str()) {
            return Err(format!("object path {:?} is duplicated", object.object).into());
        }
    }
    Ok(())
}

fn scan_object(root: &Path, object: &ManifestObject) -> Result<ObjectSummary, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let path = root.join(&object.object);
    let bytes = std::fs::read(&path)?;
    if bytes.len() as u64 != object.bytes {
        return Err(format!("holds {} bytes, manifest says {}", bytes.len(), object.bytes).into());
    }
    let digest = format!("{:x}", Sha256::digest(&bytes));
    if digest != object.sha256.to_ascii_lowercase() {
        return Err(format!("SHA-256 is {digest}, manifest says {}", object.sha256).into());
    }
    let graph = falkordb_rdb::decode_dump(&bytes)?;
    if graph.name != object.graph_utf8 {
        return Err(format!("payload graph name {:?}, manifest says {:?}", graph.name, object.graph_utf8).into());
    }
    graph.validate_reconciliation()?;
    let reconciliation = graph.reconciliation_diagnostics();
    Ok(ObjectSummary {
        object: object.object.clone(),
        nodes: graph.nodes.len(),
        edge_records: graph.edges.len(),
        relationships: graph.relationships.len(),
        relationships_with_missing_endpoints: reconciliation.relationships_with_missing_endpoints,
        relationship_cells_without_records: reconciliation.cells_without_edge_records,
        edge_records_without_cells: reconciliation.edge_records_without_cells,
        reused_edge_id_cells: reconciliation.reused_edge_id_cells,
        relationship_pairs_without_adjacency: reconciliation.relationship_pairs_without_adjacency,
        adjacency_pairs_without_relationships: reconciliation.adjacency_pairs_without_relationships,
        dangling_tensor_cells: reconciliation.dangling_tensor_cells,
        empty_tensor_cells: reconciliation.empty_tensor_cells,
        labeled_nodes: graph.labels.len(),
        adjacency_pairs: graph.adjacency.len(),
        elapsed_microseconds: started.elapsed().as_micros().min(u64::MAX as u128) as u64,
    })
}
