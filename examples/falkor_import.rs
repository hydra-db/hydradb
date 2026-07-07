use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt};
use slatedb_graph_kernel::{
    object_store_from_env, EdgeMetadata, GraphCacheConfig, GraphError, GraphLimits,
    GraphOpenOptions, GraphShard, RelationshipMutation, Result, VertexId, VertexMetadata,
    VertexPropertyValue,
};

const DEFAULT_CACHE_BYTES: u64 = 4 * 1024 * 1024 * 1024;

#[tokio::main]
async fn main() -> Result<()> {
    let config = ImportConfig::from_args(std::env::args().skip(1).collect())?;
    let started = Instant::now();
    let object_store = object_store_from_env(config.env_file.clone())?;
    let source_prefix = normalize_source_prefix(&config.source_prefix)?;

    let manifest_key = source_object_key(&source_prefix, "manifest.json");
    let nodes_key = source_object_key(&source_prefix, "nodes.jsonl");
    let edges_key = source_object_key(&source_prefix, "edges.jsonl");

    let manifest = read_object_text(Arc::clone(&object_store), &manifest_key).await?;
    let source = parse_manifest(&manifest, &manifest_key)?;
    let nodes = parse_nodes_object(Arc::clone(&object_store), &nodes_key).await?;
    let edges = scan_edges_object(
        Arc::clone(&object_store),
        &edges_key,
        config.duplicate_policy,
    )
    .await?;
    if config.duplicate_policy == DuplicatePolicy::Reject && edges.duplicate_edges > 0 {
        return Err(GraphError::UnsupportedQuery {
            dialect: "FalkorImport",
            feature: format!(
                "Falkor dump has {} parallel relationships across {} edge identities; default --duplicate-policy preserve keeps them as multigraph relationship records, while reject/collapse modes build a simple graph view",
                edges.duplicate_edges, edges.duplicate_keys
            ),
        });
    }

    let cache = config
        .cache_dir
        .as_ref()
        .map(|cache_dir| GraphCacheConfig::disk_cache(cache_dir, config.cache_bytes))
        .unwrap_or_default();
    let options = GraphOpenOptions {
        limits: GraphLimits {
            max_bulk_import_edges: config
                .edge_batch_size
                .max(config.metadata_batch_size)
                .max(1),
            max_artifact_build_edges: edges.unique_edges.max(1) as u64,
            max_query_scan_edges: edges.unique_edges.max(1) as u64,
            max_query_index_candidates: nodes.len().max(edges.unique_edges).max(1),
            ..GraphLimits::default()
        },
        cache,
        ..GraphOpenOptions::default()
    };

    let shard = GraphShard::open_standalone_writer_with_options(
        config.db_path.clone(),
        Arc::clone(&object_store),
        options,
    )
    .await?;

    let mut imported_vertices = 0_usize;
    for chunk in nodes.chunks(config.metadata_batch_size) {
        imported_vertices += shard
            .set_vertex_metadata_batch(&config.cell_id, chunk.iter().cloned())
            .await?;
    }

    let mut imported_edges = 0_u64;
    let mut imported_relationships = 0_u64;
    let mut imported_edge_metadata = 0_usize;
    let edge_import = import_edges_object(
        Arc::clone(&object_store),
        &edges_key,
        &shard,
        &config,
        &source.graph,
        &edges,
    )
    .await?;
    imported_edges += edge_import.imported_edges;
    imported_relationships += edge_import.imported_relationships;
    imported_edge_metadata += edge_import.imported_edge_metadata;

    if config.build_artifacts {
        let epoch = shard.current_epoch(&config.cell_id).await?;
        for edge_type in edges.type_counts.keys() {
            shard
                .build_posting_chunks(
                    &config.cell_id,
                    edge_type,
                    epoch,
                    config.artifact_chunk_size,
                )
                .await?;
            shard
                .build_matrix_tiles(
                    &config.cell_id,
                    edge_type,
                    epoch,
                    config.artifact_chunk_size as u64,
                )
                .await?;
            shard
                .build_supernode_groups(
                    &config.cell_id,
                    edge_type,
                    epoch,
                    config.supernode_threshold as u64,
                    config.artifact_chunk_size,
                )
                .await?;
        }
    }

    let epoch = shard.current_epoch(&config.cell_id).await?;
    shard.close().await?;

    println!(
        "falkor_import source={} graph={} cell={} db_path={} manifest_nodes={} manifest_edges={} imported_vertices={} imported_edges={} imported_relationships={} imported_edge_metadata={} unique_edges={} duplicate_edges={} duplicate_keys={} duplicate_policy={:?} edge_types={} epoch={} elapsed_ms={}",
        source_prefix,
        source.graph,
        config.cell_id,
        config.db_path,
        source.node_count,
        source.edge_count,
        imported_vertices,
        imported_edges,
        imported_relationships,
        imported_edge_metadata,
        edges.unique_edges,
        edges.duplicate_edges,
        edges.duplicate_keys,
        config.duplicate_policy,
        edges.type_counts.len(),
        epoch,
        started.elapsed().as_millis()
    );
    for (edge_type, count) in &edges.type_counts {
        println!("edge_type,{edge_type},{count}");
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct ImportConfig {
    source_prefix: String,
    env_file: Option<String>,
    db_path: String,
    cell_id: String,
    edge_batch_size: usize,
    metadata_batch_size: usize,
    duplicate_policy: DuplicatePolicy,
    build_artifacts: bool,
    artifact_chunk_size: usize,
    supernode_threshold: usize,
    cache_dir: Option<String>,
    cache_bytes: usize,
}

impl ImportConfig {
    fn from_args(args: Vec<String>) -> Result<Self> {
        let mut parser = ArgParser::new(args);
        if parser.flag("--help") || parser.flag("-h") {
            print_usage();
            std::process::exit(0);
        }
        let source_prefix = parser.required("--source-prefix")?;
        let cell_id = parser.optional("--cell-id")?.unwrap_or_else(|| {
            source_prefix
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .filter(|value| !value.is_empty())
                .unwrap_or("falkor")
                .to_string()
        });
        let db_path = parser
            .optional("--db-path")?
            .unwrap_or_else(|| format!("imports/falkor/{cell_id}"));
        let edge_batch_size = parser
            .optional_usize("--edge-batch-size")?
            .unwrap_or(16_384)
            .max(1);
        let metadata_batch_size = parser
            .optional_usize("--metadata-batch-size")?
            .unwrap_or(4_096)
            .max(1);
        let duplicate_policy = parser
            .optional("--duplicate-policy")?
            .map(|value| DuplicatePolicy::parse(&value))
            .transpose()?
            .unwrap_or(DuplicatePolicy::Preserve);
        let artifact_chunk_size = parser
            .optional_usize("--artifact-chunk-size")?
            .unwrap_or(32_768)
            .max(1);
        let supernode_threshold = parser
            .optional_usize("--supernode-threshold")?
            .unwrap_or(1_024)
            .max(1);
        let cache_bytes = parser
            .optional_usize("--cache-bytes")?
            .unwrap_or_else(default_cache_bytes);
        let config = Self {
            source_prefix,
            env_file: parser.optional("--env-file")?,
            db_path,
            cell_id,
            edge_batch_size,
            metadata_batch_size,
            duplicate_policy,
            build_artifacts: parser.flag("--build-artifacts"),
            artifact_chunk_size,
            supernode_threshold,
            cache_dir: parser.optional("--cache-dir")?,
            cache_bytes,
        };
        parser.finish()?;
        Ok(config)
    }
}

fn default_cache_bytes() -> usize {
    usize::try_from(DEFAULT_CACHE_BYTES).unwrap_or(usize::MAX)
}

struct ArgParser {
    args: Vec<String>,
}

impl ArgParser {
    fn new(args: Vec<String>) -> Self {
        Self { args }
    }

    fn required(&mut self, name: &str) -> Result<String> {
        self.optional(name)?
            .ok_or_else(|| GraphError::UnsupportedQuery {
                dialect: "FalkorImport",
                feature: format!("missing required argument {name}"),
            })
    }

    fn optional(&mut self, name: &str) -> Result<Option<String>> {
        let Some(idx) = self.args.iter().position(|arg| arg == name) else {
            return Ok(None);
        };
        self.args.remove(idx);
        if idx >= self.args.len() || self.args[idx].starts_with('-') {
            return Err(GraphError::UnsupportedQuery {
                dialect: "FalkorImport",
                feature: format!("{name} requires a value"),
            });
        }
        let value = self.args.remove(idx);
        if value.trim().is_empty() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "FalkorImport",
                feature: format!("{name} cannot be empty"),
            });
        }
        Ok(Some(value))
    }

    fn optional_usize(&mut self, name: &str) -> Result<Option<usize>> {
        self.optional(name)?
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|err| GraphError::UnsupportedQuery {
                        dialect: "FalkorImport",
                        feature: format!("{name} must be a positive integer: {err}"),
                    })
            })
            .transpose()
    }

    fn flag(&mut self, name: &str) -> bool {
        match self.args.iter().position(|arg| arg == name) {
            Some(idx) => {
                self.args.remove(idx);
                true
            }
            None => false,
        }
    }

    fn finish(self) -> Result<()> {
        if self.args.is_empty() {
            Ok(())
        } else {
            Err(GraphError::UnsupportedQuery {
                dialect: "FalkorImport",
                feature: format!("unknown arguments: {}", self.args.join(" ")),
            })
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DuplicatePolicy {
    Preserve,
    Reject,
    CollapseFirst,
    CollapseLast,
}

impl DuplicatePolicy {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "preserve" => Ok(Self::Preserve),
            "reject" => Ok(Self::Reject),
            "collapse-first" => Ok(Self::CollapseFirst),
            "collapse-last" => Ok(Self::CollapseLast),
            _ => Err(GraphError::UnsupportedQuery {
                dialect: "FalkorImport",
                feature: format!(
                    "unsupported duplicate policy {value}; expected preserve, reject, collapse-first, or collapse-last"
                ),
            }),
        }
    }
}

#[derive(Debug)]
struct Manifest {
    graph: String,
    node_count: u64,
    edge_count: u64,
}

#[derive(Clone, Debug)]
struct ParsedEdge {
    relationship_id: u64,
    src: VertexId,
    dst: VertexId,
    metadata: EdgeMetadata,
}

#[derive(Clone, Debug)]
struct ParsedEdgeLine {
    edge_type: String,
    edge: ParsedEdge,
}

type EdgeIdentity = (String, VertexId, VertexId);

#[derive(Default, Debug)]
struct ParsedEdges {
    type_counts: BTreeMap<String, usize>,
    identity_counts: BTreeMap<EdgeIdentity, usize>,
    unique_edges: usize,
    duplicate_edges: usize,
    duplicate_keys: usize,
}

#[derive(Default, Debug)]
struct EdgeImportTotals {
    imported_edges: u64,
    imported_relationships: u64,
    imported_edge_metadata: usize,
}

#[derive(Default, Debug)]
struct EdgeTypeWriteState {
    chunk_idx: usize,
    records: Vec<ParsedEdge>,
}

fn parse_manifest(text: &str, key: &str) -> Result<Manifest> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|err| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("invalid manifest JSON: {err}"),
        })?;
    Ok(Manifest {
        graph: json_string(&value, "graph", key)?.to_string(),
        node_count: json_u64(&value, "node_count", key)?,
        edge_count: json_u64(&value, "edge_count", key)?,
    })
}

async fn parse_nodes_object(
    object_store: Arc<dyn ObjectStore>,
    key: &str,
) -> Result<Vec<(VertexId, VertexMetadata)>> {
    let mut nodes = Vec::new();
    let mut seen = BTreeSet::new();
    stream_jsonl_lines(object_store, key, |line_no, line| {
        let line_key = format!("{key}:{line_no}");
        let value: serde_json::Value =
            serde_json::from_str(line).map_err(|err| GraphError::CorruptValue {
                key: line_key.clone(),
                reason: format!("invalid node JSON: {err}"),
            })?;
        let id = json_u64(&value, "id", &line_key)?;
        if !seen.insert(id) {
            return Err(GraphError::CorruptValue {
                key: line_key,
                reason: format!("duplicate node id {id}"),
            });
        }
        let mut metadata = VertexMetadata::default();
        let labels = value
            .get("labels")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| GraphError::CorruptValue {
                key: line_key.clone(),
                reason: "node labels must be an array".to_string(),
            })?;
        for label in labels {
            metadata.labels.insert(
                label
                    .as_str()
                    .ok_or_else(|| GraphError::CorruptValue {
                        key: line_key.clone(),
                        reason: "node label must be a string".to_string(),
                    })?
                    .to_string(),
            );
        }
        metadata.properties = parse_properties(value.get("properties"), &line_key)?;
        metadata
            .properties
            .entry("_fid".to_string())
            .or_insert(VertexPropertyValue::Integer(id));
        nodes.push((id, metadata));
        Ok(())
    })
    .await?;
    Ok(nodes)
}

async fn scan_edges_object(
    object_store: Arc<dyn ObjectStore>,
    key: &str,
    duplicate_policy: DuplicatePolicy,
) -> Result<ParsedEdges> {
    let mut identity_counts = BTreeMap::<EdgeIdentity, usize>::new();
    let mut type_counts = BTreeMap::<String, usize>::new();

    stream_jsonl_lines(object_store, key, |line_no, line| {
        let parsed = parse_edge_line(key, line_no, line)?;
        let identity = edge_identity(&parsed.edge_type, &parsed.edge);
        *type_counts.entry(parsed.edge_type.clone()).or_default() += 1;
        let count = identity_counts.entry(identity).or_default();
        *count += 1;
        if duplicate_policy == DuplicatePolicy::Reject && *count > 1 {
            return Err(GraphError::UnsupportedQuery {
                dialect: "FalkorImport",
                feature: format!(
                    "Falkor dump contains parallel relationship {}:{} -> {}; use --duplicate-policy preserve to keep multigraph records or collapse-first/collapse-last for a simple graph view",
                    parsed.edge_type, parsed.edge.src, parsed.edge.dst
                ),
            });
        }
        Ok(())
    })
    .await?;

    let duplicate_edges = identity_counts
        .values()
        .map(|count| count.saturating_sub(1))
        .sum::<usize>();
    let duplicate_keys = identity_counts.values().filter(|count| **count > 1).count();
    let unique_edges = identity_counts.len();
    Ok(ParsedEdges {
        type_counts,
        identity_counts,
        unique_edges,
        duplicate_edges,
        duplicate_keys,
    })
}

async fn import_edges_object(
    object_store: Arc<dyn ObjectStore>,
    key: &str,
    shard: &GraphShard,
    config: &ImportConfig,
    source_graph: &str,
    summary: &ParsedEdges,
) -> Result<EdgeImportTotals> {
    if config.duplicate_policy == DuplicatePolicy::CollapseLast {
        return import_collapse_last_edges_object(
            object_store,
            key,
            shard,
            config,
            source_graph,
            summary,
        )
        .await;
    }

    let path = Path::from(key.to_string());
    let raw = object_store.get(&path).await?;
    let mut stream = raw.into_stream();
    let mut buffer = Vec::new();
    let mut line_no = 0_usize;
    let mut states = BTreeMap::<String, EdgeTypeWriteState>::new();
    let mut first_seen = BTreeSet::<EdgeIdentity>::new();
    let mut totals = EdgeImportTotals::default();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buffer.extend_from_slice(&chunk);
        while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            let mut line = buffer.drain(..=newline).collect::<Vec<_>>();
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            line_no += 1;
            import_edge_line(
                key,
                line_no,
                &line,
                shard,
                config,
                source_graph,
                summary,
                &mut states,
                &mut first_seen,
                &mut totals,
            )
            .await?;
        }
    }
    if !buffer.is_empty() {
        line_no += 1;
        import_edge_line(
            key,
            line_no,
            &buffer,
            shard,
            config,
            source_graph,
            summary,
            &mut states,
            &mut first_seen,
            &mut totals,
        )
        .await?;
    }

    for (edge_type, mut state) in states {
        flush_edge_type_state(
            shard,
            config,
            source_graph,
            &edge_type,
            &mut state,
            &mut totals,
        )
        .await?;
    }
    Ok(totals)
}

async fn import_collapse_last_edges_object(
    object_store: Arc<dyn ObjectStore>,
    key: &str,
    shard: &GraphShard,
    config: &ImportConfig,
    source_graph: &str,
    summary: &ParsedEdges,
) -> Result<EdgeImportTotals> {
    let mut last_by_identity = BTreeMap::<EdgeIdentity, ParsedEdge>::new();
    stream_jsonl_lines(object_store, key, |line_no, line| {
        let parsed = parse_edge_line(key, line_no, line)?;
        last_by_identity.insert(edge_identity(&parsed.edge_type, &parsed.edge), parsed.edge);
        Ok(())
    })
    .await?;

    let mut states = BTreeMap::<String, EdgeTypeWriteState>::new();
    let mut totals = EdgeImportTotals::default();
    for (identity, edge) in last_by_identity {
        let edge_type = identity.0.clone();
        let edge = edge_with_parallel_count(edge, &identity, summary, config.duplicate_policy);
        push_edge_for_import(
            shard,
            config,
            source_graph,
            &mut states,
            &mut totals,
            edge_type,
            edge,
        )
        .await?;
    }
    for (edge_type, mut state) in states {
        flush_edge_type_state(
            shard,
            config,
            source_graph,
            &edge_type,
            &mut state,
            &mut totals,
        )
        .await?;
    }
    Ok(totals)
}

async fn import_edge_line(
    key: &str,
    line_no: usize,
    line: &[u8],
    shard: &GraphShard,
    config: &ImportConfig,
    source_graph: &str,
    summary: &ParsedEdges,
    states: &mut BTreeMap<String, EdgeTypeWriteState>,
    first_seen: &mut BTreeSet<EdgeIdentity>,
    totals: &mut EdgeImportTotals,
) -> Result<()> {
    let Some(line) = decode_jsonl_line(key, line_no, line)? else {
        return Ok(());
    };
    let parsed = parse_edge_line(key, line_no, line)?;
    let identity = edge_identity(&parsed.edge_type, &parsed.edge);
    if config.duplicate_policy == DuplicatePolicy::CollapseFirst
        && !first_seen.insert(identity.clone())
    {
        return Ok(());
    }
    let edge = edge_with_parallel_count(parsed.edge, &identity, summary, config.duplicate_policy);
    push_edge_for_import(
        shard,
        config,
        source_graph,
        states,
        totals,
        parsed.edge_type,
        edge,
    )
    .await
}

async fn push_edge_for_import(
    shard: &GraphShard,
    config: &ImportConfig,
    source_graph: &str,
    states: &mut BTreeMap<String, EdgeTypeWriteState>,
    totals: &mut EdgeImportTotals,
    edge_type: String,
    edge: ParsedEdge,
) -> Result<()> {
    let should_flush = {
        let state = states.entry(edge_type.clone()).or_default();
        state.records.push(edge);
        state.records.len() >= config.edge_batch_size
    };
    if should_flush {
        let mut state = states
            .remove(&edge_type)
            .ok_or_else(|| GraphError::CorruptValue {
                key: format!("falkor_import/{edge_type}"),
                reason: "missing edge import state after flush trigger".to_string(),
            })?;
        flush_edge_type_state(shard, config, source_graph, &edge_type, &mut state, totals).await?;
        states.insert(edge_type, state);
    }
    Ok(())
}

async fn flush_edge_type_state(
    shard: &GraphShard,
    config: &ImportConfig,
    source_graph: &str,
    edge_type: &str,
    state: &mut EdgeTypeWriteState,
    totals: &mut EdgeImportTotals,
) -> Result<()> {
    if state.records.is_empty() {
        return Ok(());
    }
    let chunk_idx = state.chunk_idx;
    if config.duplicate_policy == DuplicatePolicy::Preserve {
        let result = shard
            .import_relationships_batch(
                &config.cell_id,
                edge_type,
                state.records.iter().map(|edge| RelationshipMutation {
                    cell_id: config.cell_id.clone(),
                    edge_type: edge_type.to_string(),
                    src: edge.src,
                    dst: edge.dst,
                    relationship_id: edge.relationship_id,
                    metadata: edge.metadata.clone(),
                }),
                &format!(
                    "falkor-rel-{}-{}-{chunk_idx}",
                    component_slug(source_graph),
                    component_slug(edge_type)
                ),
            )
            .await?;
        totals.imported_edges += result.structural_edges_inserted;
        totals.imported_relationships += result.relationships_inserted;
    } else {
        let structural_edges: Vec<_> = state
            .records
            .iter()
            .map(|edge| (edge.src, edge.dst))
            .collect();
        let result = shard
            .bulk_import_edges_chunked(
                &config.cell_id,
                edge_type,
                structural_edges,
                &format!(
                    "falkor-{}-{}-{chunk_idx}",
                    component_slug(source_graph),
                    component_slug(edge_type)
                ),
                config.edge_batch_size,
            )
            .await?;
        totals.imported_edges += result.inserted;
        totals.imported_edge_metadata += shard
            .set_edge_metadata_batch(
                &config.cell_id,
                edge_type,
                state
                    .records
                    .iter()
                    .map(|edge| (edge.src, edge.dst, edge.metadata.clone())),
            )
            .await?;
    }
    state.records.clear();
    state.chunk_idx += 1;
    Ok(())
}

fn parse_edge_line(key: &str, line_no: usize, line: &str) -> Result<ParsedEdgeLine> {
    let line_key = format!("{key}:{line_no}");
    let value: serde_json::Value =
        serde_json::from_str(line).map_err(|err| GraphError::CorruptValue {
            key: line_key.clone(),
            reason: format!("invalid edge JSON: {err}"),
        })?;
    let edge_id = json_u64(&value, "id", &line_key)?;
    let edge_type = json_string(&value, "type", &line_key)?.to_string();
    let src = json_u64(&value, "source_id", &line_key)?;
    let dst = json_u64(&value, "target_id", &line_key)?;
    let mut metadata = EdgeMetadata {
        properties: parse_properties(value.get("properties"), &line_key)?,
    };
    metadata
        .properties
        .entry("_fid".to_string())
        .or_insert(VertexPropertyValue::Integer(edge_id));
    Ok(ParsedEdgeLine {
        edge_type,
        edge: ParsedEdge {
            relationship_id: edge_id,
            src,
            dst,
            metadata,
        },
    })
}

fn edge_identity(edge_type: &str, edge: &ParsedEdge) -> EdgeIdentity {
    (edge_type.to_string(), edge.src, edge.dst)
}

fn edge_with_parallel_count(
    mut edge: ParsedEdge,
    identity: &EdgeIdentity,
    summary: &ParsedEdges,
    duplicate_policy: DuplicatePolicy,
) -> ParsedEdge {
    if duplicate_policy != DuplicatePolicy::Reject {
        if let Some(count) = summary.identity_counts.get(identity).copied() {
            if count > 1 {
                edge.metadata.properties.insert(
                    "_falkor_parallel_count".to_string(),
                    VertexPropertyValue::Integer(count as u64),
                );
            }
        }
    }
    edge
}

fn parse_properties(
    value: Option<&serde_json::Value>,
    key: &str,
) -> Result<BTreeMap<String, VertexPropertyValue>> {
    let Some(value) = value else {
        return Ok(BTreeMap::new());
    };
    let object = value.as_object().ok_or_else(|| GraphError::CorruptValue {
        key: key.to_string(),
        reason: "properties must be a JSON object".to_string(),
    })?;
    Ok(object
        .iter()
        .map(|(property, value)| {
            (
                property.clone(),
                VertexPropertyValue::from_json_value(value),
            )
        })
        .collect())
}

async fn read_object_text(object_store: Arc<dyn ObjectStore>, key: &str) -> Result<String> {
    let path = Path::from(key.to_string());
    let raw = object_store.get(&path).await?;
    let bytes = raw.bytes().await?;
    String::from_utf8(bytes.to_vec()).map_err(|err| GraphError::CorruptValue {
        key: key.to_string(),
        reason: format!("object is not valid UTF-8: {err}"),
    })
}

async fn stream_jsonl_lines(
    object_store: Arc<dyn ObjectStore>,
    key: &str,
    mut on_line: impl FnMut(usize, &str) -> Result<()>,
) -> Result<()> {
    let path = Path::from(key.to_string());
    let raw = object_store.get(&path).await?;
    let mut stream = raw.into_stream();
    let mut buffer = Vec::new();
    let mut line_no = 0_usize;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buffer.extend_from_slice(&chunk);
        while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            let mut line = buffer.drain(..=newline).collect::<Vec<_>>();
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            line_no += 1;
            emit_jsonl_line(key, line_no, &line, &mut on_line)?;
        }
    }
    if !buffer.is_empty() {
        line_no += 1;
        emit_jsonl_line(key, line_no, &buffer, &mut on_line)?;
    }
    Ok(())
}

fn emit_jsonl_line(
    key: &str,
    line_no: usize,
    line: &[u8],
    on_line: &mut impl FnMut(usize, &str) -> Result<()>,
) -> Result<()> {
    let Some(line) = decode_jsonl_line(key, line_no, line)? else {
        return Ok(());
    };
    on_line(line_no, line)
}

fn decode_jsonl_line<'a>(key: &str, line_no: usize, line: &'a [u8]) -> Result<Option<&'a str>> {
    let line = std::str::from_utf8(line).map_err(|err| GraphError::CorruptValue {
        key: format!("{key}:{line_no}"),
        reason: format!("line is not valid UTF-8: {err}"),
    })?;
    if line.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(line))
}

fn normalize_source_prefix(source: &str) -> Result<String> {
    let source = source.trim();
    if source.is_empty() {
        return Err(GraphError::UnsupportedQuery {
            dialect: "FalkorImport",
            feature: "--source-prefix cannot be empty".to_string(),
        });
    }
    let source = source.trim_end_matches('/');
    if let Some(rest) = source.strip_prefix("s3://") {
        let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err(GraphError::UnsupportedQuery {
                dialect: "FalkorImport",
                feature: "s3 source prefix must include a bucket name".to_string(),
            });
        }
        return Ok(key.trim_matches('/').to_string());
    }
    Ok(source.trim_matches('/').to_string())
}

fn source_object_key(prefix: &str, file_name: &str) -> String {
    let file_name = file_name.trim_start_matches('/');
    if prefix.is_empty() {
        file_name.to_string()
    } else {
        format!("{}/{file_name}", prefix.trim_end_matches('/'))
    }
}

fn json_string<'a>(value: &'a serde_json::Value, field: &str, key: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("{field} must be a string"),
        })
}

fn json_u64(value: &serde_json::Value, field: &str, key: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| GraphError::CorruptValue {
            key: key.to_string(),
            reason: format!("{field} must be an unsigned integer"),
        })
}

fn component_slug(value: &str) -> String {
    value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.') {
                byte as char
            } else {
                '_'
            }
        })
        .collect()
}

fn print_usage() {
    eprintln!(
        "usage: cargo run --features json-properties --example falkor_import -- \\
         --source-prefix orgs/<org>/<graph> [--env-file .env] [--db-path imports/falkor/<graph>] \\
         [--cell-id <graph>] [--duplicate-policy preserve|reject|collapse-first|collapse-last] \\
         [--edge-batch-size 16384] [--metadata-batch-size 4096] [--build-artifacts]"
    );
}
