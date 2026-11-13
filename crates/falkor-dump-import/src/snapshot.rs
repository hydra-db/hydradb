use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufReader, BufWriter};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{merge_scopes, resolve_with_source_scope, GraphResolution, QuarantinedVertex, ResolveError, ResolvedScope};

const SNAPSHOT_FORMAT: &str = "falkordb-redis-dump-per-graph-v1";

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("read {path}: {source}")]
    Read { path: PathBuf, source: std::io::Error },
    #[error("write {path}: {source}")]
    Write { path: PathBuf, source: std::io::Error },
    #[error("parse {path}: {source}")]
    Json { path: PathBuf, source: serde_json::Error },
    #[error("invalid snapshot manifest: {0}")]
    Manifest(String),
    #[error("{object}: {message}")]
    Object { object: String, message: String },
    #[error("encode or decode {path}: {message}")]
    Codec { path: PathBuf, message: String },
    #[error("resolve snapshot: {0}")]
    Resolve(#[from] ResolveError),
    /// Every graph that could not be decoded or resolved: a planning pass
    /// goes on past one, so a run reports them all instead of the first.
    #[error("{} graph(s) failed to decode or resolve:\n{}", .0.len(), .0.join("\n"))]
    Graphs(Vec<String>),
}

#[derive(Debug)]
pub struct SnapshotPlan {
    pub snapshot_id: String,
    pub manifest_sha256: String,
    pub object_count: usize,
    pub dump_bytes: u64,
    pub scopes: Vec<ResolvedScope>,
    /// Vertices no scope can hold, for the quarantine report.
    pub unscoped_vertices: Vec<QuarantinedVertex>,
    pub elapsed: Duration,
}

#[derive(Debug)]
pub struct SnapshotSpill {
    pub snapshot_id: String,
    pub manifest_sha256: String,
    pub object_count: usize,
    pub dump_bytes: u64,
    pub raw_scopes: usize,
    pub raw_vertices: u64,
    pub raw_relationships: u64,
    pub orphan_edges: u64,
    pub relationship_anomalies: u64,
    /// Vertices no scope can hold, for the quarantine report, spilled per
    /// graph like the scopes so no step holds them all at once.
    pub unscoped_vertices: SpilledUnscoped,
    pub included_graphs: Vec<SnapshotGraph>,
    pub excluded_graphs: Vec<ExcludedGraph>,
    pub selected_object_count: usize,
    pub selected_dump_bytes: u64,
    pub scopes: Vec<SpilledScope>,
    pub elapsed: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SnapshotGraph {
    pub graph: String,
    pub object: String,
    pub bytes: u64,
    pub sha256: String,
}

/// Backwards-compatible name for the exclusion audit record.
pub type ExcludedGraph = SnapshotGraph;

/// Exact source graph names, never logical collection scopes or patterns.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum GraphSelection {
    #[default]
    All,
    Include(BTreeSet<String>),
    Exclude(BTreeSet<String>),
}

impl GraphSelection {
    pub fn add_graph(&mut self, option: &str, graph: String) -> Result<(), SnapshotError> {
        if graph.trim().is_empty() || graph.starts_with("--") {
            return Err(SnapshotError::Manifest(format!("{option} requires a non-empty graph name")));
        }
        let included = match option {
            "--include-graph" => true,
            "--exclude-graph" => false,
            _ => return Err(SnapshotError::Manifest(format!("unknown graph selection option {option}"))),
        };
        if matches!(self, Self::All) {
            *self = if included {
                Self::Include(BTreeSet::new())
            } else {
                Self::Exclude(BTreeSet::new())
            };
        }
        let graphs = match self {
            Self::Include(graphs) if included => graphs,
            Self::Exclude(graphs) if !included => graphs,
            _ => return Err(SnapshotError::Manifest("cannot combine --include-graph and --exclude-graph".into())),
        };
        if !graphs.insert(graph.clone()) {
            return Err(SnapshotError::Manifest(format!("{option} {graph:?} is duplicated")));
        }
        Ok(())
    }

    pub fn included_names(&self) -> Vec<String> {
        match self {
            Self::Include(graphs) => graphs.iter().cloned().collect(),
            _ => Vec::new(),
        }
    }

    fn selects(&self, graph: &str) -> bool {
        match self {
            Self::All => true,
            Self::Include(graphs) => graphs.contains(graph),
            Self::Exclude(graphs) => !graphs.contains(graph),
        }
    }
}

#[derive(Debug)]
pub struct SpilledScope {
    pub scope_sha256: String,
    pub fragments: Vec<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    format: String,
    snapshot_id: String,
    dumped_graphs: usize,
    dump_bytes: u64,
    objects: Vec<Object>,
    #[serde(skip)]
    sha256: String,
}

#[derive(Debug, Deserialize)]
struct Object {
    object: String,
    graph_utf8: String,
    bytes: u64,
    sha256: String,
    status: String,
}

#[derive(Debug)]
struct Fragment {
    scope_sha256: String,
    path: PathBuf,
    vertices: u64,
    relationships: u64,
    orphan_edges: u64,
    relationship_anomalies: u64,
}

pub fn build_snapshot_plan(root: &Path, workers: usize) -> Result<SnapshotPlan, SnapshotError> {
    let manifest = load_manifest(root)?;
    let object_count = manifest.objects.len();
    let objects = Arc::new(manifest.objects);
    let next = Arc::new(AtomicUsize::new(0));
    let outputs = Arc::new(Mutex::new(Vec::<ResolvedScope>::new()));
    let unscoped = Arc::new(Mutex::new(Vec::<QuarantinedVertex>::new()));
    let failure = Arc::new(Mutex::new(None::<SnapshotError>));
    let graph_failures = Arc::new(Mutex::new(Vec::<String>::new()));
    let started = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..workers.max(1).min(object_count.max(1)) {
            let root = root.to_path_buf();
            let objects = Arc::clone(&objects);
            let next = Arc::clone(&next);
            let outputs = Arc::clone(&outputs);
            let unscoped = Arc::clone(&unscoped);
            let failure = Arc::clone(&failure);
            let graph_failures = Arc::clone(&graph_failures);
            scope.spawn(move || loop {
                if failure.lock().expect("snapshot failure mutex poisoned").is_some() {
                    break;
                }
                let index = next.fetch_add(1, Ordering::Relaxed);
                let Some(object) = objects.get(index) else {
                    break;
                };
                match graph_failure(object, decode_object(&root, object, SourceLayout::Shared)) {
                    Err(message) => graph_failures.lock().expect("snapshot failure mutex poisoned").push(message),
                    Ok(Ok(mut resolved)) => {
                        outputs.lock().expect("snapshot output mutex poisoned").append(&mut resolved.scopes);
                        unscoped
                            .lock()
                            .expect("snapshot output mutex poisoned")
                            .append(&mut resolved.unscoped_vertices);
                    }
                    Ok(Err(error)) => {
                        *failure.lock().expect("snapshot failure mutex poisoned") = Some(error);
                        break;
                    }
                }
            });
        }
    });
    if let Some(error) = failure.lock().expect("snapshot failure mutex poisoned").take() {
        return Err(error);
    }
    take_graph_failures(&graph_failures)?;
    let outputs = Arc::try_unwrap(outputs)
        .map_err(|_| SnapshotError::Manifest("snapshot output remains shared".into()))?
        .into_inner()
        .map_err(|_| SnapshotError::Manifest("snapshot output mutex poisoned".into()))?;
    Ok(SnapshotPlan {
        snapshot_id: manifest.snapshot_id,
        manifest_sha256: manifest.sha256,
        object_count,
        dump_bytes: manifest.dump_bytes,
        scopes: merge_scopes(outputs)?,
        unscoped_vertices: take_unscoped(unscoped)?,
        elapsed: started.elapsed(),
    })
}

pub fn build_snapshot_spill(root: &Path, workers: usize, scratch: &Path) -> Result<SnapshotSpill, SnapshotError> {
    build_snapshot_spill_excluding(root, workers, scratch, &BTreeSet::new())
}

pub fn build_snapshot_spill_excluding(
    root: &Path,
    workers: usize,
    scratch: &Path,
    excluded_graphs: &BTreeSet<String>,
) -> Result<SnapshotSpill, SnapshotError> {
    let selection = if excluded_graphs.is_empty() {
        GraphSelection::All
    } else {
        GraphSelection::Exclude(excluded_graphs.clone())
    };
    build_snapshot_spill_selected(root, workers, scratch, &selection)
}

pub fn build_snapshot_spill_selected(
    root: &Path,
    workers: usize,
    scratch: &Path,
    selection: &GraphSelection,
) -> Result<SnapshotSpill, SnapshotError> {
    build_snapshot_spill_with_layout(root, workers, scratch, selection, None)
}

/// One dedicated database per manifest. The selected physical graph keys and the
/// configured tenant endpoint jointly determine placement, including the tenant-named key.
#[derive(Clone, Debug, Deserialize, serde::Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DedicatedLayout {
    pub tenant_id: String,
    #[serde(deserialize_with = "unique_graph_map")]
    pub graphs: BTreeMap<String, String>,
}

fn unique_graph_map<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<BTreeMap<String, String>, D::Error> {
    struct UniqueGraphs;
    impl<'de> serde::de::Visitor<'de> for UniqueGraphs {
        type Value = BTreeMap<String, String>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a mapping with unique physical graph names")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut graphs = BTreeMap::new();
            while let Some((key, value)) = map.next_entry()? {
                if graphs.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate physical graph in dedicated layout"));
                }
            }
            Ok(graphs)
        }
    }
    deserializer.deserialize_map(UniqueGraphs)
}

impl DedicatedLayout {
    pub fn read(path: &Path) -> Result<Self, SnapshotError> {
        let file = std::fs::File::open(path).map_err(|source| SnapshotError::Read { path: path.into(), source })?;
        serde_json::from_reader(file).map_err(|source| SnapshotError::Json { path: path.into(), source })
    }

    fn validate<'a>(&self, names: impl Iterator<Item = &'a str>) -> Result<(), SnapshotError> {
        if self.tenant_id.trim().is_empty() || self.tenant_id.chars().any(char::is_control) || self.graphs.is_empty() {
            return Err(SnapshotError::Manifest(
                "dedicated layout requires a tenant and at least one graph".into(),
            ));
        }
        let mut seen = BTreeSet::new();
        for graph in names {
            let Some(sub) = self.graphs.get(graph) else {
                return Err(SnapshotError::Manifest(format!(
                    "selected graph {graph:?} is absent from dedicated layout"
                )));
            };
            if graph.trim().is_empty()
                || graph.chars().any(char::is_control)
                || graph.starts_with("byog:")
                || graph != (if sub.is_empty() { &self.tenant_id } else { sub })
                || !seen.insert(graph)
            {
                return Err(SnapshotError::Manifest(format!(
                    "invalid or duplicate dedicated managed graph {graph:?}"
                )));
            }
        }
        if seen.len() != self.graphs.len() {
            return Err(SnapshotError::Manifest(
                "dedicated layout includes graphs absent from selection".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub enum SourceLayout<'a> {
    #[default]
    Shared,
    Dedicated(&'a DedicatedLayout),
    Byog,
}

pub fn build_snapshot_spill_with_layout(
    root: &Path,
    workers: usize,
    scratch: &Path,
    selection: &GraphSelection,
    layout: Option<&DedicatedLayout>,
) -> Result<SnapshotSpill, SnapshotError> {
    build_snapshot_spill_with_source_layout(
        root,
        workers,
        scratch,
        selection,
        layout.map_or(SourceLayout::Shared, SourceLayout::Dedicated),
    )
}

pub fn build_snapshot_spill_with_source_layout(
    root: &Path,
    workers: usize,
    scratch: &Path,
    selection: &GraphSelection,
    layout: SourceLayout<'_>,
) -> Result<SnapshotSpill, SnapshotError> {
    let manifest = load_manifest(root)?;
    let mut included = Vec::new();
    let mut excluded = Vec::new();
    let (requested, description) = match selection {
        GraphSelection::All => (None, "selected"),
        GraphSelection::Include(graphs) if graphs.is_empty() => {
            return Err(SnapshotError::Manifest("include selection cannot be empty".into()));
        }
        GraphSelection::Include(graphs) => (Some(graphs), "included"),
        GraphSelection::Exclude(graphs) => (Some(graphs), "excluded"),
    };
    if let Some(requested) = requested {
        let mut matches = BTreeMap::<&str, Vec<&Object>>::new();
        for object in &manifest.objects {
            if requested.contains(&object.graph_utf8) {
                matches.entry(&object.graph_utf8).or_default().push(object);
            }
        }
        for graph in requested {
            if graph.trim().is_empty() {
                return Err(SnapshotError::Manifest("selected graph name cannot be empty".into()));
            }
            let object = match matches.get(graph.as_str()).map(Vec::as_slice) {
                None | Some([]) => {
                    return Err(SnapshotError::Manifest(format!(
                        "{description} graph {graph:?} is absent from the snapshot"
                    )))
                }
                Some([object]) => *object,
                Some(objects) => {
                    return Err(SnapshotError::Manifest(format!(
                        "{description} graph {graph:?} appears in {} snapshot objects",
                        objects.len()
                    )))
                }
            };
            let audit = SnapshotGraph {
                graph: graph.clone(),
                object: object.object.clone(),
                bytes: object.bytes,
                sha256: object.sha256.to_ascii_lowercase(),
            };
            if matches!(selection, GraphSelection::Exclude(_)) {
                // Preserve exclusion provenance checks. Include mode never
                // opens unselected payloads; selected payloads are fully
                // verified by decode_object before a plan can succeed.
                inspect_excluded_object(root, object)?;
                excluded.push(audit);
            } else {
                included.push(audit);
            }
        }
    }
    std::fs::create_dir_all(scratch).map_err(|source| SnapshotError::Write {
        path: scratch.to_path_buf(),
        source,
    })?;
    let object_count = manifest.objects.len();
    let objects = Arc::new(
        manifest
            .objects
            .into_iter()
            .filter(|object| selection.selects(&object.graph_utf8))
            .collect::<Vec<_>>(),
    );
    if let SourceLayout::Dedicated(layout) = layout {
        layout.validate(objects.iter().map(|object| object.graph_utf8.as_str()))?;
    }
    if matches!(layout, SourceLayout::Byog) {
        let mut scopes = BTreeSet::new();
        for object in objects.iter() {
            let scope = crate::byog_scope(&object.graph_utf8)?;
            if !scopes.insert(scope) {
                return Err(SnapshotError::Manifest(format!("duplicate BYOG graph {:?}", object.graph_utf8)));
            }
        }
    }
    let selected_object_count = objects.len();
    let selected_dump_bytes = objects.iter().map(|object| object.bytes).sum();
    let next = Arc::new(AtomicUsize::new(0));
    let outputs = Arc::new(Mutex::new(Vec::<Fragment>::new()));
    let unscoped = Arc::new(Mutex::new(SpilledUnscoped::default()));
    let failure = Arc::new(Mutex::new(None::<SnapshotError>));
    let graph_failures = Arc::new(Mutex::new(Vec::<String>::new()));
    let started = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..workers.max(1).min(selected_object_count.max(1)) {
            let root = root.to_path_buf();
            let scratch = scratch.to_path_buf();
            let objects = Arc::clone(&objects);
            let next = Arc::clone(&next);
            let outputs = Arc::clone(&outputs);
            let unscoped = Arc::clone(&unscoped);
            let failure = Arc::clone(&failure);
            let graph_failures = Arc::clone(&graph_failures);
            scope.spawn(move || loop {
                if failure.lock().expect("snapshot failure mutex poisoned").is_some() {
                    break;
                }
                let index = next.fetch_add(1, Ordering::Relaxed);
                let Some(object) = objects.get(index) else {
                    break;
                };
                let decoded = match graph_failure(object, decode_object(&root, object, layout)) {
                    Err(message) => {
                        graph_failures.lock().expect("snapshot failure mutex poisoned").push(message);
                        continue;
                    }
                    Ok(decoded) => decoded,
                };
                let result = decoded.and_then(|resolved| {
                    unscoped
                        .lock()
                        .expect("snapshot output mutex poisoned")
                        .spill(&scratch, index, resolved.unscoped_vertices)?;
                    spill_object(&scratch, index, resolved.scopes)
                });
                match result {
                    Ok(mut fragments) => outputs.lock().expect("snapshot output mutex poisoned").append(&mut fragments),
                    Err(error) => {
                        *failure.lock().expect("snapshot failure mutex poisoned") = Some(error);
                        break;
                    }
                }
            });
        }
    });
    if let Some(error) = failure.lock().expect("snapshot failure mutex poisoned").take() {
        return Err(error);
    }
    take_graph_failures(&graph_failures)?;
    let fragments = Arc::try_unwrap(outputs)
        .map_err(|_| SnapshotError::Manifest("snapshot output remains shared".into()))?
        .into_inner()
        .map_err(|_| SnapshotError::Manifest("snapshot output mutex poisoned".into()))?;
    let raw_scopes = fragments.len();
    let raw_vertices = fragments.iter().map(|fragment| fragment.vertices).sum();
    let raw_relationships = fragments.iter().map(|fragment| fragment.relationships).sum();
    let orphan_edges = fragments.iter().map(|fragment| fragment.orphan_edges).sum();
    let relationship_anomalies = fragments.iter().map(|fragment| fragment.relationship_anomalies).sum();
    let mut grouped = BTreeMap::<String, Vec<PathBuf>>::new();
    for fragment in fragments {
        grouped.entry(fragment.scope_sha256).or_default().push(fragment.path);
    }
    let scopes = grouped
        .into_iter()
        .map(|(scope_sha256, mut fragments)| {
            fragments.sort();
            SpilledScope { scope_sha256, fragments }
        })
        .collect();
    Ok(SnapshotSpill {
        snapshot_id: manifest.snapshot_id,
        manifest_sha256: manifest.sha256,
        object_count,
        dump_bytes: manifest.dump_bytes,
        raw_scopes,
        raw_vertices,
        raw_relationships,
        orphan_edges,
        relationship_anomalies,
        unscoped_vertices: {
            let mut unscoped = Arc::try_unwrap(unscoped)
                .map_err(|_| SnapshotError::Manifest("snapshot output remains shared".into()))?
                .into_inner()
                .map_err(|_| SnapshotError::Manifest("snapshot output mutex poisoned".into()))?;
            // Workers finish graphs out of order; the file name is the object index.
            unscoped.fragments.sort();
            unscoped
        },
        included_graphs: included,
        excluded_graphs: excluded,
        selected_object_count,
        selected_dump_bytes,
        scopes,
        elapsed: started.elapsed(),
    })
}

/// The vertices no scope can hold, as one scratch file per graph that had
/// any, in snapshot object order and sorted by source within each.
#[derive(Debug, Default)]
pub struct SpilledUnscoped {
    pub count: u64,
    pub fragments: Vec<PathBuf>,
}

impl SpilledUnscoped {
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Spill one graph's unscoped vertices (the snapshot object at
    /// `object_index`) and record the file. Nothing is written for none.
    pub fn spill(&mut self, scratch: &Path, object_index: usize, mut vertices: Vec<QuarantinedVertex>) -> Result<(), SnapshotError> {
        if vertices.is_empty() {
            return Ok(());
        }
        vertices.sort_by(|left, right| left.source.cmp(&right.source));
        let directory = scratch.join("unscoped");
        std::fs::create_dir_all(&directory).map_err(|source| SnapshotError::Write {
            path: directory.clone(),
            source,
        })?;
        let path = directory.join(format!("{object_index:08}.bin"));
        let temporary = path.with_extension("bin.tmp");
        let file = std::fs::File::create(&temporary).map_err(|source| SnapshotError::Write {
            path: temporary.clone(),
            source,
        })?;
        rmp_serde::encode::write_named(&mut BufWriter::new(file), &vertices).map_err(|source| SnapshotError::Codec {
            path: temporary.clone(),
            message: source.to_string(),
        })?;
        std::fs::rename(&temporary, &path).map_err(|source| SnapshotError::Write {
            path: path.clone(),
            source,
        })?;
        self.count += vertices.len() as u64;
        self.fragments.push(path);
        Ok(())
    }

    /// One graph's vertices, from a file [`Self::spill`] wrote.
    pub fn load(path: &Path) -> Result<Vec<QuarantinedVertex>, SnapshotError> {
        let file = std::fs::File::open(path).map_err(|source| SnapshotError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        rmp_serde::from_read(BufReader::new(file)).map_err(|source| SnapshotError::Codec {
            path: path.to_path_buf(),
            message: source.to_string(),
        })
    }
}

pub fn load_spilled_scope(scope: &SpilledScope) -> Result<ResolvedScope, SnapshotError> {
    let mut inputs = Vec::with_capacity(scope.fragments.len());
    for path in &scope.fragments {
        let file = std::fs::File::open(path).map_err(|source| SnapshotError::Read {
            path: path.clone(),
            source,
        })?;
        inputs.push(rmp_serde::from_read(BufReader::new(file)).map_err(|source| SnapshotError::Codec {
            path: path.clone(),
            message: source.to_string(),
        })?);
    }
    let mut merged = merge_scopes(inputs)?;
    if merged.len() != 1 {
        return Err(SnapshotError::Manifest(format!(
            "scope spill {} resolved to {} scopes",
            scope.scope_sha256,
            merged.len()
        )));
    }
    let resolved = merged.pop().expect("length checked");
    if scope_digest(&resolved) != scope.scope_sha256 {
        return Err(SnapshotError::Manifest(format!(
            "scope spill {} contains a different scope",
            scope.scope_sha256
        )));
    }
    Ok(resolved)
}

fn spill_object(scratch: &Path, object_index: usize, scopes: Vec<ResolvedScope>) -> Result<Vec<Fragment>, SnapshotError> {
    let mut fragments = Vec::with_capacity(scopes.len());
    for (scope_index, scope) in scopes.into_iter().enumerate() {
        let scope_sha256 = scope_digest(&scope);
        let directory = scratch.join(&scope_sha256);
        std::fs::create_dir_all(&directory).map_err(|source| SnapshotError::Write {
            path: directory.clone(),
            source,
        })?;
        let path = directory.join(format!("{object_index:08}-{scope_index:08}.bin"));
        let temporary = path.with_extension("bin.tmp");
        let file = std::fs::File::create(&temporary).map_err(|source| SnapshotError::Write {
            path: temporary.clone(),
            source,
        })?;
        rmp_serde::encode::write_named(&mut BufWriter::new(file), &scope).map_err(|source| SnapshotError::Codec {
            path: temporary.clone(),
            message: source.to_string(),
        })?;
        std::fs::rename(&temporary, &path).map_err(|source| SnapshotError::Write {
            path: path.clone(),
            source,
        })?;
        fragments.push(Fragment {
            scope_sha256,
            path,
            vertices: scope.vertices.len() as u64,
            relationships: scope.relationships.len() as u64,
            orphan_edges: scope.orphan_edges.len() as u64,
            relationship_anomalies: scope.relationship_anomalies.len() as u64,
        });
    }
    Ok(fragments)
}

fn scope_digest(scope: &ResolvedScope) -> String {
    format!("{:x}", Sha256::digest(scope.scope.to_string().as_bytes()))
}

fn load_manifest(root: &Path) -> Result<Manifest, SnapshotError> {
    let manifest_path = root.join("manifest.json");
    let manifest_bytes = std::fs::read(&manifest_path).map_err(|source| SnapshotError::Read {
        path: manifest_path.clone(),
        source,
    })?;
    let mut manifest: Manifest = serde_json::from_slice(&manifest_bytes).map_err(|source| SnapshotError::Json {
        path: manifest_path,
        source,
    })?;
    manifest.sha256 = format!("{:x}", Sha256::digest(&manifest_bytes));
    validate_manifest(&manifest)?;
    Ok(manifest)
}

fn validate_manifest(manifest: &Manifest) -> Result<(), SnapshotError> {
    if manifest.format != SNAPSHOT_FORMAT {
        return Err(SnapshotError::Manifest(format!(
            "format is {}, expected {SNAPSHOT_FORMAT}",
            manifest.format
        )));
    }
    if manifest.dumped_graphs != manifest.objects.len() {
        return Err(SnapshotError::Manifest(format!(
            "dumped_graphs is {}, object count is {}",
            manifest.dumped_graphs,
            manifest.objects.len()
        )));
    }
    let bytes: u64 = manifest.objects.iter().map(|object| object.bytes).sum();
    if bytes != manifest.dump_bytes {
        return Err(SnapshotError::Manifest(format!(
            "dump_bytes is {}, object total is {bytes}",
            manifest.dump_bytes
        )));
    }
    if let Some(object) = manifest.objects.iter().find(|object| object.status != "dumped") {
        return Err(SnapshotError::Manifest(format!("{} has status {}", object.object, object.status)));
    }
    let mut names = BTreeSet::new();
    for object in &manifest.objects {
        let path = Path::new(&object.object);
        if path.as_os_str().is_empty() || path.components().any(|component| !matches!(component, Component::Normal(_))) {
            return Err(SnapshotError::Manifest(format!(
                "object path {:?} is not a safe relative path",
                object.object
            )));
        }
        if !names.insert(object.object.as_str()) {
            return Err(SnapshotError::Manifest(format!("object path {:?} is duplicated", object.object)));
        }
    }
    Ok(())
}

fn take_unscoped(unscoped: Arc<Mutex<Vec<QuarantinedVertex>>>) -> Result<Vec<QuarantinedVertex>, SnapshotError> {
    let mut vertices = Arc::try_unwrap(unscoped)
        .map_err(|_| SnapshotError::Manifest("snapshot output remains shared".into()))?
        .into_inner()
        .map_err(|_| SnapshotError::Manifest("snapshot output mutex poisoned".into()))?;
    vertices.sort_by(|left, right| left.source.cmp(&right.source));
    Ok(vertices)
}

/// A graph that does not decode or resolve, as the line a planning pass
/// reports it by: a pass notes it and goes on, so one run names every such
/// graph. Any other outcome, a failure to read included, is passed on.
fn graph_failure(
    object: &Object,
    decoded: Result<GraphResolution, SnapshotError>,
) -> Result<Result<GraphResolution, SnapshotError>, String> {
    match decoded {
        Err(error @ SnapshotError::Object { .. }) => Err(error.to_string()),
        Err(error @ SnapshotError::Resolve(_)) => Err(format!("{}: {error}", object.object)),
        decoded => Ok(decoded),
    }
}

/// The graphs a planning pass could not decode or resolve, as one error.
fn take_graph_failures(failures: &Mutex<Vec<String>>) -> Result<(), SnapshotError> {
    let mut failures = std::mem::take(&mut *failures.lock().expect("snapshot failure mutex poisoned"));
    if failures.is_empty() {
        return Ok(());
    }
    failures.sort();
    Err(SnapshotError::Graphs(failures))
}

fn decode_object(root: &Path, object: &Object, layout: SourceLayout<'_>) -> Result<GraphResolution, SnapshotError> {
    let bytes = validate_object_blob(root, object)?;
    let graph = falkordb_rdb::decode_dump(&bytes).map_err(|error| object_error(object, error.to_string()))?;
    drop(bytes);
    if graph.name != object.graph_utf8 {
        return Err(object_error(
            object,
            format!("payload graph name {:?}, manifest declares {:?}", graph.name, object.graph_utf8),
        ));
    }
    let unexplained = graph.adjacency_pairs_without_relationships();
    if unexplained > 0 {
        eprintln!(
            "falkor-dump-import: {}: {unexplained} adjacency pair(s) without a typed relationship, not imported",
            object.object
        );
    }
    if matches!(layout, SourceLayout::Byog) {
        return crate::resolve_byog_graph(graph).map_err(SnapshotError::Resolve);
    }
    if graph.name.starts_with("byog:") {
        return Err(object_error(
            object,
            "BYOG graph requires --byog; managed resolution would rewrite its data",
        ));
    }
    let source_scope = match layout {
        SourceLayout::Dedicated(layout) => Some(crate::Scope {
            tenant_id: layout.tenant_id.clone(),
            sub_tenant_id: layout.graphs[&object.graph_utf8].clone(),
        }),
        _ => None,
    };
    resolve_with_source_scope(graph, source_scope.as_ref()).map_err(SnapshotError::Resolve)
}

fn validate_object_blob(root: &Path, object: &Object) -> Result<Vec<u8>, SnapshotError> {
    let path = root.join(&object.object);
    let bytes = std::fs::read(&path).map_err(|source| SnapshotError::Read { path, source })?;
    if bytes.len() as u64 != object.bytes {
        return Err(object_error(
            object,
            format!("holds {} bytes, manifest declares {}", bytes.len(), object.bytes),
        ));
    }
    let digest = format!("{:x}", Sha256::digest(&bytes));
    if digest != object.sha256.to_ascii_lowercase() {
        return Err(object_error(
            object,
            format!("SHA-256 is {digest}, manifest declares {}", object.sha256),
        ));
    }
    Ok(bytes)
}

fn inspect_excluded_object(root: &Path, object: &Object) -> Result<falkordb_rdb::DumpIdentity, SnapshotError> {
    let path = root.join(&object.object);
    let mut file = std::fs::File::open(&path).map_err(|source| SnapshotError::Read {
        path: path.clone(),
        source,
    })?;
    let identity = falkordb_rdb::inspect_dump_identity(&mut file).map_err(|error| object_error(object, error.to_string()))?;
    if identity.bytes != object.bytes {
        return Err(object_error(
            object,
            format!("holds {} bytes, manifest declares {}", identity.bytes, object.bytes),
        ));
    }
    if identity.sha256 != object.sha256.to_ascii_lowercase() {
        return Err(object_error(
            object,
            format!("SHA-256 is {}, manifest declares {}", identity.sha256, object.sha256),
        ));
    }
    if identity.name != object.graph_utf8 {
        return Err(object_error(
            object,
            format!("payload graph name {:?}, manifest declares {:?}", identity.name, object.graph_utf8),
        ));
    }
    Ok(identity)
}

fn object_error(object: &Object, message: impl Into<String>) -> SnapshotError {
    SnapshotError::Object {
        object: object.object.clone(),
        message: message.into(),
    }
}
