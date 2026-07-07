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

#[tokio::main]
async fn main() -> Result<()> {
    let config = ImportConfig::from_args(std::env::args().skip(1).collect())?;
    let started = Instant::now();
    let object_store = object_store_from_env(config.env_file.clone())?;
    let source_prefix = normalize_source_prefix(&config.source_prefix);

    let manifest = read_object_text(
        Arc::clone(&object_store),
        &format!("{source_prefix}/manifest.json"),
    )
    .await?;
    let source = parse_manifest(&manifest, &source_prefix)?;
    let nodes = parse_nodes_object(
        Arc::clone(&object_store),
        &format!("{source_prefix}/nodes.jsonl"),
    )
    .await?;
    let edges = parse_edges_object(
        Arc::clone(&object_store),
        &format!("{source_prefix}/edges.jsonl"),
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
    for (edge_type, records) in &edges.by_type {
        if config.duplicate_policy == DuplicatePolicy::Preserve {
            for (chunk_idx, chunk) in records.chunks(config.edge_batch_size).enumerate() {
                let result = shard
                    .import_relationships_batch(
                        &config.cell_id,
                        edge_type,
                        chunk.iter().map(|edge| RelationshipMutation {
                            cell_id: config.cell_id.clone(),
                            edge_type: edge_type.clone(),
                            src: edge.src,
                            dst: edge.dst,
                            relationship_id: edge.relationship_id,
                            metadata: edge.metadata.clone(),
                        }),
                        &format!(
                            "falkor-rel-{}-{}-{chunk_idx}",
                            component_slug(&source.graph),
                            component_slug(edge_type)
                        ),
                    )
                    .await?;
                imported_edges += result.structural_edges_inserted;
                imported_relationships += result.relationships_inserted;
            }
        } else {
            for (chunk_idx, chunk) in records.chunks(config.edge_batch_size).enumerate() {
                let structural_edges: Vec<_> =
                    chunk.iter().map(|edge| (edge.src, edge.dst)).collect();
                let result = shard
                    .bulk_import_edges_chunked(
                        &config.cell_id,
                        edge_type,
                        structural_edges,
                        &format!(
                            "falkor-{}-{}-{chunk_idx}",
                            component_slug(&source.graph),
                            component_slug(edge_type)
                        ),
                        config.edge_batch_size,
                    )
                    .await?;
                imported_edges += result.inserted;
                imported_edge_metadata += shard
                    .set_edge_metadata_batch(
                        &config.cell_id,
                        edge_type,
                        chunk
                            .iter()
                            .map(|edge| (edge.src, edge.dst, edge.metadata.clone())),
                    )
                    .await?;
            }
        }
    }

    if config.build_artifacts {
        let epoch = shard.current_epoch(&config.cell_id).await?;
        for edge_type in edges.by_type.keys() {
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
        edges.by_type.len(),
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
        let cell_id = parser.optional("--cell-id").unwrap_or_else(|| {
            source_prefix
                .rsplit('/')
                .next()
                .unwrap_or("falkor")
                .to_string()
        });
        let db_path = parser
            .optional("--db-path")
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
            .optional("--duplicate-policy")
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
            .unwrap_or(4 * 1024 * 1024 * 1024);
        let config = Self {
            source_prefix,
            env_file: parser.optional("--env-file"),
            db_path,
            cell_id,
            edge_batch_size,
            metadata_batch_size,
            duplicate_policy,
            build_artifacts: parser.flag("--build-artifacts"),
            artifact_chunk_size,
            supernode_threshold,
            cache_dir: parser.optional("--cache-dir"),
            cache_bytes,
        };
        parser.finish()?;
        Ok(config)
    }
}

struct ArgParser {
    args: Vec<String>,
}

impl ArgParser {
    fn new(args: Vec<String>) -> Self {
        Self { args }
    }

    fn required(&mut self, name: &str) -> Result<String> {
        self.optional(name)
            .ok_or_else(|| GraphError::UnsupportedQuery {
                dialect: "FalkorImport",
                feature: format!("missing required argument {name}"),
            })
    }

    fn optional(&mut self, name: &str) -> Option<String> {
        let idx = self.args.iter().position(|arg| arg == name)?;
        self.args.remove(idx);
        if idx >= self.args.len() {
            return Some(String::new());
        }
        Some(self.args.remove(idx))
    }

    fn optional_usize(&mut self, name: &str) -> Result<Option<usize>> {
        self.optional(name)
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

#[derive(Default, Debug)]
struct ParsedEdges {
    by_type: BTreeMap<String, Vec<ParsedEdge>>,
    type_counts: BTreeMap<String, usize>,
    unique_edges: usize,
    duplicate_edges: usize,
    duplicate_keys: usize,
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

async fn parse_edges_object(
    object_store: Arc<dyn ObjectStore>,
    key: &str,
    duplicate_policy: DuplicatePolicy,
) -> Result<ParsedEdges> {
    let mut raw_by_identity = BTreeMap::<(String, VertexId, VertexId), ParsedEdge>::new();
    let mut raw_counts = BTreeMap::<(String, VertexId, VertexId), usize>::new();
    let mut type_counts = BTreeMap::<String, usize>::new();
    let mut relationship_ids = BTreeSet::new();

    stream_jsonl_lines(object_store, key, |line_no, line| {
        let line_key = format!("{key}:{line_no}");
        let value: serde_json::Value =
            serde_json::from_str(line).map_err(|err| GraphError::CorruptValue {
                key: line_key.clone(),
                reason: format!("invalid edge JSON: {err}"),
            })?;
        let edge_id = json_u64(&value, "id", &line_key)?;
        if !relationship_ids.insert(edge_id) {
            return Err(GraphError::CorruptValue {
                key: line_key,
                reason: format!("duplicate relationship id {edge_id}"),
            });
        }
        let edge_type = json_string(&value, "type", &line_key)?.to_string();
        let src = json_u64(&value, "source_id", &line_key)?;
        let dst = json_u64(&value, "target_id", &line_key)?;
        *type_counts.entry(edge_type.clone()).or_default() += 1;

        let mut metadata = EdgeMetadata {
            properties: parse_properties(value.get("properties"), &line_key)?,
        };
        metadata
            .properties
            .entry("_fid".to_string())
            .or_insert(VertexPropertyValue::Integer(edge_id));

        let parsed = ParsedEdge {
            relationship_id: edge_id,
            src,
            dst,
            metadata,
        };
        let identity = (edge_type, src, dst);
        let count = raw_counts.entry(identity.clone()).or_default();
        *count += 1;
        if duplicate_policy == DuplicatePolicy::Preserve {
            raw_by_identity.insert((format!("{}\0{edge_id:020}", identity.0), src, dst), parsed);
            return Ok(());
        }
        match raw_by_identity.get_mut(&identity) {
            None => {
                raw_by_identity.insert(identity, parsed);
            }
            Some(existing) => match duplicate_policy {
                DuplicatePolicy::Preserve
                | DuplicatePolicy::Reject
                | DuplicatePolicy::CollapseFirst => {}
                DuplicatePolicy::CollapseLast => {
                    *existing = parsed;
                }
            },
        }
        Ok(())
    })
    .await?;

    let duplicate_edges = raw_counts
        .values()
        .map(|count| count.saturating_sub(1))
        .sum::<usize>();
    let duplicate_keys = raw_counts.values().filter(|count| **count > 1).count();
    let mut by_type = BTreeMap::<String, Vec<ParsedEdge>>::new();
    for ((edge_type, src, dst), mut edge) in raw_by_identity {
        let edge_type = match edge_type.split_once('\0') {
            Some((edge_type, _)) => edge_type.to_string(),
            None => edge_type,
        };
        if let Some(count) = raw_counts.get(&(edge_type.clone(), src, dst)).copied() {
            if count > 1 && duplicate_policy != DuplicatePolicy::Reject {
                edge.metadata.properties.insert(
                    "_falkor_parallel_count".to_string(),
                    VertexPropertyValue::Integer(count as u64),
                );
            }
        }
        by_type.entry(edge_type).or_default().push(edge);
    }
    let unique_edges = raw_counts.len();
    Ok(ParsedEdges {
        by_type,
        type_counts,
        unique_edges,
        duplicate_edges,
        duplicate_keys,
    })
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
    let line = std::str::from_utf8(line).map_err(|err| GraphError::CorruptValue {
        key: format!("{key}:{line_no}"),
        reason: format!("line is not valid UTF-8: {err}"),
    })?;
    if line.trim().is_empty() {
        return Ok(());
    }
    on_line(line_no, line)
}

fn normalize_source_prefix(source: &str) -> String {
    let source = source.trim_end_matches('/');
    if let Some(rest) = source.strip_prefix("s3://") {
        return rest
            .split_once('/')
            .map(|(_, key)| key.trim_end_matches('/').to_string())
            .unwrap_or_default();
    }
    source.to_string()
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
