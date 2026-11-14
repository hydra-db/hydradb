//! `graph-image`: build a graph store offline as a SlateDB image in the online
//! writers' key layout, check a store against the inputs it was built from,
//! and digest any store for comparison with another.
//!
//! - `build` reads a vertex spec and an edge list, streams the image in key
//!   order into the offline builder, and finalizes it (seal, generations,
//!   verification, statistics, scope registration) through the crate's
//!   shared `finalize_image`. A declared last slice of vertices and edges can
//!   be held back for a loader to write online.
//! - `verify` reads every vertex and edge family of a store back and compares
//!   it with the rows the full spec and edge list produce.
//! - `digest` prints, per key family, a count and hashes, plus the store's
//!   shape, generations and statistics.
//!
//! `verify` and `digest` open the store read-only with the kernel's
//! follow-latest reader options, so pointing them at a published store cannot
//! change it.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use falkor_dump_import::{
    finalize_image, offline_import_limits, ExternalKvSorter, ImageFinalize, SingleWriterLocalStore, SortedRows, VerifiedEdgeType,
};
use hydradb::{
    encode_planner_statistics, graph_scope_for_database, object_store_from_env, DatabaseName, GraphError, GraphId, GraphIndexGeneration,
    GraphLimits, GraphOpenOptions, GraphScope, GraphShard, NamespacePath, ObjectStoreGraphScopeDirectory, OfflineGraphImageEncoder,
    OfflineImageRow, PlannerStatistics, PlannerStatisticsBuildLimits, VertexMetadata, VertexPropertyValue,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::bytes::Bytes;
use slatedb::object_store::path::Path as ObjectPath;
use slatedb::object_store::{ObjectStore, ObjectStoreExt, PutMode};
use slatedb::{OfflineImageBuilder, OfflineImageOptions};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// An image's edges by type, each type's in (src, dst) order.
type EdgesByType = BTreeMap<String, Vec<(u64, u64)>>;

/// Bumped when a receipt's meaning changes.
const RECEIPT_MAGIC: &str = "hydradb-graph-image-v1";
/// One `edges.bin` record: little-endian `u64 src`, `u64 dst`, `u16 type`.
const EDGE_RECORD_BYTES: usize = 18;
/// Vertex ids a worker encodes per chunk of a streamed family.
const VERTEX_CHUNK: u64 = 16_384;
/// Chunks each worker may have encoded ahead of the builder.
const CHUNKS_AHEAD: usize = 4;
/// Sort-buffer memory for the rows that are not streamed in id order: the
/// edge side and any vertex family whose keys are not monotone in the id.
const SORT_BUDGET_BYTES: usize = 2 * 1024 * 1024 * 1024;
/// `max_bulk_import_edges` for the finalize shard. Nothing in finalize
/// imports edges in batches, so this is the falkor importer's default.
const FINALIZE_BATCH_SIZE: usize = 8_192;
/// The indexer's statistics limits, read the way it reads them, with its
/// defaults (`PlannerStatisticsBuildLimits::default()`).
const STATISTICS_MAX_ENTRIES_VAR: &str = "GRAPH_PLANNER_STATISTICS_MAX_ENTRIES";
const STATISTICS_EXACT_CAP_VAR: &str = "GRAPH_PLANNER_STATISTICS_EXACT_CAP";
/// The write path an image mirrors: the cypher engine whose `UNWIND`
/// lowering reaches `UpsertVertices` and `CreateEdges`. Under DataFusion the
/// batches take another path, which the writer-equivalence test does not
/// cover.
const MIRRORED_CYPHER_ENGINE: &str = "experimental";

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() {
    if let Err(error) = run(std::env::args().skip(1).collect()).await {
        eprintln!("graph-image: {error}");
        std::process::exit(1);
    }
}

async fn run(arguments: Vec<String>) -> Result<(), BoxError> {
    let command = Command::parse(arguments)?;
    let object_store = object_store_from_env(command.env_file().map(str::to_string))?;
    // The identity is read after the store: the env file, when given, is
    // loaded into the process environment by the store's loader.
    let values = std::env::vars().collect::<BTreeMap<_, _>>();
    let target = Target::from_values(&values, command.database())?;
    let output = match command {
        Command::Build(args) => {
            // A local directory cannot do the conditional update statistics
            // publication needs; this process is its only writer, so it does
            // it itself, as the falkor importer does.
            let object_store: Arc<dyn ObjectStore> = if values.get("CLOUD_PROVIDER").map(String::as_str) == Some("local") {
                Arc::new(SingleWriterLocalStore::new(object_store))
            } else {
                object_store
            };
            serde_json::to_value(build(&args, &target, statistics_limits(&values)?, object_store).await?)?
        }
        Command::Verify(args) => serde_json::to_value(verify(&args, &target, object_store).await?)?,
        Command::Digest(args) if args.statistics_header => serde_json::to_value(statistics_header(&target, object_store).await?)?,
        Command::Digest(_) => serde_json::to_value(digest(&target, statistics_limits(&values)?, object_store).await?)?,
    };
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

enum Command {
    Build(BuildArgs),
    Verify(VerifyArgs),
    Digest(DigestArgs),
}

struct BuildArgs {
    inputs: InputArgs,
    hold_back_percent: u64,
    sst_target_bytes: usize,
    scratch_dir: Option<PathBuf>,
}

struct VerifyArgs {
    inputs: InputArgs,
    scratch_dir: Option<PathBuf>,
}

struct DigestArgs {
    common: CommonArgs,
    statistics_header: bool,
}

struct InputArgs {
    common: CommonArgs,
    vertex_spec: PathBuf,
    edges: PathBuf,
}

struct CommonArgs {
    database: String,
    env_file: Option<String>,
}

const USAGE: &str = "usage:
  graph-image build --vertex-spec SPEC.json --edges DIR/edges.bin --database DB [--hold-back-percent P] [--sst-target-bytes N] [--scratch-dir DIR] [--env-file FILE]
  graph-image verify --vertex-spec SPEC.json --edges DIR/edges.bin --database DB [--scratch-dir DIR] [--env-file FILE]
  graph-image digest --database DB [--statistics-header] [--env-file FILE]
the store and the root scope come from the env graph-node reads: GRAPH_NAMESPACE, GRAPH_ID, GRAPH_CELL_ID,
GRAPH_DATA_PATH, GRAPH_DATABASE and the object store variables";

impl Command {
    fn parse(arguments: Vec<String>) -> Result<Self, BoxError> {
        let mut arguments = arguments.into_iter();
        let subcommand = arguments.next().ok_or(USAGE)?;
        let mut values = BTreeMap::new();
        let mut statistics_header = false;
        while let Some(argument) = arguments.next() {
            if argument == "--statistics-header" && subcommand == "digest" {
                statistics_header = true;
                continue;
            }
            if !argument.starts_with("--") {
                return Err(format!("unexpected argument {argument}\n{USAGE}").into());
            }
            let value = arguments.next().ok_or_else(|| format!("{argument} requires a value"))?;
            if values.insert(argument.clone(), value).is_some() {
                return Err(format!("{argument} given twice").into());
            }
        }
        let common = CommonArgs {
            database: values.remove("--database").ok_or("missing required --database")?,
            env_file: values.remove("--env-file"),
        };
        let mut inputs = |common| -> Result<InputArgs, BoxError> {
            Ok(InputArgs {
                common,
                vertex_spec: PathBuf::from(values.remove("--vertex-spec").ok_or("missing required --vertex-spec")?),
                edges: PathBuf::from(values.remove("--edges").ok_or("missing required --edges")?),
            })
        };
        let command = match subcommand.as_str() {
            "build" => {
                let inputs = inputs(common)?;
                let hold_back_percent = values
                    .remove("--hold-back-percent")
                    .map(|value| value.parse::<u64>())
                    .transpose()
                    .map_err(|error| format!("invalid --hold-back-percent: {error}"))?
                    .unwrap_or(0);
                if hold_back_percent >= 100 {
                    return Err("--hold-back-percent must be below 100".into());
                }
                let sst_target_bytes = values
                    .remove("--sst-target-bytes")
                    .map(|value| value.parse::<usize>())
                    .transpose()
                    .map_err(|error| format!("invalid --sst-target-bytes: {error}"))?
                    .unwrap_or(hydradb::GRAPH_SST_TARGET_BYTES);
                if sst_target_bytes == 0 {
                    return Err("--sst-target-bytes must be positive".into());
                }
                Command::Build(BuildArgs {
                    inputs,
                    hold_back_percent,
                    sst_target_bytes,
                    scratch_dir: values.remove("--scratch-dir").map(PathBuf::from),
                })
            }
            "verify" => Command::Verify(VerifyArgs {
                inputs: inputs(common)?,
                scratch_dir: values.remove("--scratch-dir").map(PathBuf::from),
            }),
            "digest" => Command::Digest(DigestArgs { common, statistics_header }),
            other => return Err(format!("unknown subcommand {other}\n{USAGE}").into()),
        };
        if !values.is_empty() {
            return Err(format!("unknown options: {}", values.keys().cloned().collect::<Vec<_>>().join(", ")).into());
        }
        Ok(command)
    }

    fn common(&self) -> &CommonArgs {
        match self {
            Command::Build(args) => &args.inputs.common,
            Command::Verify(args) => &args.inputs.common,
            Command::Digest(args) => &args.common,
        }
    }

    fn database(&self) -> &str {
        &self.common().database
    }

    fn env_file(&self) -> Option<&str> {
        self.common().env_file.as_deref()
    }
}

/// The store a database name selects, resolved as the node resolves it.
#[derive(Clone, Debug)]
struct Target {
    base_path: String,
    cell_id: String,
    root: GraphScope,
    scope: GraphScope,
    database: String,
}

impl Target {
    fn from_values(values: &BTreeMap<String, String>, database: &str) -> Result<Self, BoxError> {
        let value = |name: &str, default: &str| values.get(name).cloned().unwrap_or_else(|| default.to_string());
        let root = GraphScope::new(
            NamespacePath::parse(&value("GRAPH_NAMESPACE", "default"))?,
            GraphId::new(value("GRAPH_ID", "default"))?,
        );
        let base_database = DatabaseName::parse(&value("GRAPH_DATABASE", "default"))?;
        let scope = graph_scope_for_database(&base_database, &root, &DatabaseName::parse(database)?)?;
        Ok(Self {
            base_path: value("GRAPH_DATA_PATH", "graph/data"),
            cell_id: value("GRAPH_CELL_ID", "cell-0"),
            root,
            scope,
            database: database.to_string(),
        })
    }

    fn shard_path(&self) -> String {
        format!("{}/{}", self.scope.scoped_store_path(&self.base_path), self.cell_id)
    }

    fn receipt_path(&self) -> ObjectPath {
        let base = self.base_path.trim_matches('/');
        let receipt = "_offline_imports/graph-image/receipt.json";
        if base.is_empty() {
            receipt.into()
        } else {
            format!("{base}/{receipt}").into()
        }
    }

    fn cell_prefix(&self) -> String {
        format!("cell/{}/", self.cell_id)
    }
}

fn statistics_limits(values: &BTreeMap<String, String>) -> Result<PlannerStatisticsBuildLimits, BoxError> {
    let defaults = PlannerStatisticsBuildLimits::default();
    let positive = |name: &str, default: u64| -> Result<u64, BoxError> {
        let Some(value) = values.get(name) else {
            return Ok(default);
        };
        let parsed = value
            .trim()
            .parse::<u64>()
            .map_err(|error| format!("{name} must be a whole number: {error}"))?;
        if parsed == 0 {
            return Err(format!("{name} must be greater than zero").into());
        }
        Ok(parsed)
    };
    Ok(PlannerStatisticsBuildLimits {
        max_entries: positive(STATISTICS_MAX_ENTRIES_VAR, defaults.max_entries)?,
        exact_entries: positive(STATISTICS_EXACT_CAP_VAR, defaults.exact_entries)?,
        deadline: None,
        ..defaults
    })
}

/// The vertices of an image: a contiguous id range, the labels every vertex
/// gets and the properties, each a formula of the id. No `id` property: the
/// id is the vertex id, as the online `MERGE` lowering makes it.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct VertexSpec {
    first: u64,
    count: u64,
    #[serde(default)]
    labels: BTreeSet<String>,
    #[serde(default)]
    properties: BTreeMap<String, Generator>,
}

/// A property's value as a formula of the vertex id.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
enum Generator {
    /// id * k
    Mul(i64),
    /// id + c
    Add(i64),
    Const(ConstValue),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum ConstValue {
    Integer(i64),
    Bool(bool),
    String(String),
}

impl VertexSpec {
    fn read(path: &Path) -> Result<Self, BoxError> {
        let spec: Self =
            serde_json::from_slice(&std::fs::read(path)?).map_err(|error| format!("invalid vertex spec {}: {error}", path.display()))?;
        if spec.count == 0 {
            return Err("the vertex spec has no vertices".into());
        }
        spec.first
            .checked_add(spec.count - 1)
            .ok_or("the vertex spec's id range overflows u64")?;
        if spec.properties.contains_key("id") {
            return Err("the vertex spec sets `id`, which is the vertex id and never a stored property".into());
        }
        // Every value in range must be representable before anything is
        // written: the formulas are monotone, so the ends bound them.
        for id in [spec.first, spec.last()] {
            spec.metadata(id)?;
        }
        Ok(spec)
    }

    fn last(&self) -> u64 {
        self.first + (self.count - 1)
    }

    fn metadata(&self, id: u64) -> Result<VertexMetadata, BoxError> {
        let properties = self
            .properties
            .iter()
            .map(|(name, generator)| Ok((name.clone(), generator.value(id)?)))
            .collect::<Result<BTreeMap<_, _>, BoxError>>()?;
        Ok(VertexMetadata {
            labels: self.labels.clone(),
            properties,
        })
    }
}

impl Generator {
    /// The value for `id`, as a bolt integer reaches the store: an integer
    /// is `Integer(u64)` when >= 0 and `SignedInteger` only when negative
    /// (`VertexPropertyValue::from_i64`), and anything outside `i64` is
    /// refused, since no bolt client could have sent it.
    fn value(&self, id: u64) -> Result<VertexPropertyValue, BoxError> {
        let integer = |value: i128| -> Result<VertexPropertyValue, BoxError> {
            let value = i64::try_from(value).map_err(|_| format!("{self:?} gives {value} for vertex {id}, outside a 64-bit integer"))?;
            Ok(VertexPropertyValue::from_i64(value))
        };
        match self {
            Generator::Mul(k) => integer(i128::from(id) * i128::from(*k)),
            Generator::Add(c) => integer(i128::from(id) + i128::from(*c)),
            Generator::Const(ConstValue::Integer(value)) => Ok(VertexPropertyValue::from_i64(*value)),
            Generator::Const(ConstValue::Bool(value)) => Ok(VertexPropertyValue::Bool(*value)),
            Generator::Const(ConstValue::String(value)) => Ok(VertexPropertyValue::String(value.clone())),
        }
    }

    /// Whether this property's index keys (`{value key}/{id:020}`) increase
    /// with the id over `first..=last`, so its family can stream in id order
    /// unsorted. An unsigned integer's key is `i{value:020}` and a negative
    /// one's `j{..}`, each order-preserving within its sign, but every `i`
    /// key sorts before every `j` key, so a range that crosses zero is not
    /// monotone. A constant is one value key, followed by the id.
    fn monotone(&self, first: u64, last: u64) -> bool {
        match self {
            Generator::Mul(k) => *k >= 0,
            Generator::Add(c) => i128::from(first) + i128::from(*c) >= 0 || i128::from(last) + i128::from(*c) < 0,
            Generator::Const(_) => true,
        }
    }
}

/// `edges.json`, beside `edges.bin`: the type names in index order, the
/// counts, and a sha256 over `"{src},{dst},{type};"` in file order. Other
/// fields a producer adds are ignored.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct EdgeListHeader {
    types: Vec<String>,
    edges: u64,
    edges_by_type: BTreeMap<String, u64>,
    sha256: String,
}

#[derive(Clone, Copy, Debug)]
struct EdgeEntry {
    src: u64,
    dst: u64,
    /// The record's position in `edges.bin`.
    position: u32,
    /// The type's rank by name, so entries sort in key order.
    rank: u16,
}

/// Every edge of an edge list, sorted by (type name, src, dst), checked
/// whole: digest, counts, types, no self loop, endpoints in the spec's id
/// range, no duplicate.
struct EdgeList {
    header: EdgeListHeader,
    /// Type names by rank.
    types: Vec<String>,
    edges: Vec<EdgeEntry>,
}

impl EdgeList {
    fn read(path: &Path, spec: &VertexSpec) -> Result<Self, BoxError> {
        let header_path = path.with_extension("json");
        let header: EdgeListHeader = serde_json::from_slice(&std::fs::read(&header_path)?)
            .map_err(|error| format!("invalid edge list header {}: {error}", header_path.display()))?;
        let length = std::fs::metadata(path)?.len();
        if length % EDGE_RECORD_BYTES as u64 != 0 {
            return Err(format!(
                "{} is {length} bytes, not a whole number of {EDGE_RECORD_BYTES}-byte records",
                path.display()
            )
            .into());
        }
        let records = length / EDGE_RECORD_BYTES as u64;
        if records != header.edges {
            return Err(format!(
                "{} holds {records} records, {} says {}",
                path.display(),
                header_path.display(),
                header.edges
            )
            .into());
        }
        // A record's position is kept as a u32.
        if records > u64::from(u32::MAX) {
            return Err(format!("{records} edge records is more than this tool indexes ({})", u32::MAX).into());
        }
        if header.types.is_empty() || header.types.len() > usize::from(u16::MAX) + 1 {
            return Err(format!("{} must name between 1 and 65536 edge types", header_path.display()).into());
        }
        let mut names = header.types.iter().enumerate().collect::<Vec<_>>();
        names.sort_by(|left, right| left.1.cmp(right.1));
        if names.windows(2).any(|pair| pair[0].1 == pair[1].1) {
            return Err(format!("{} names an edge type twice", header_path.display()).into());
        }
        let mut rank_of_index = vec![0_u16; header.types.len()];
        for (rank, (index, _)) in names.iter().enumerate() {
            rank_of_index[*index] = rank as u16;
        }
        let types = names.iter().map(|(_, name)| (*name).clone()).collect::<Vec<_>>();

        let mut reader = BufReader::with_capacity(8 << 20, std::fs::File::open(path)?);
        let mut digest = Sha256::new();
        let mut per_type = vec![0_u64; header.types.len()];
        let mut edges = Vec::with_capacity(records as usize);
        let mut record = [0_u8; EDGE_RECORD_BYTES];
        let mut text = String::new();
        for position in 0..records as u32 {
            reader.read_exact(&mut record)?;
            let src = u64::from_le_bytes(record[0..8].try_into().expect("eight bytes"));
            let dst = u64::from_le_bytes(record[8..16].try_into().expect("eight bytes"));
            let edge_type = u16::from_le_bytes(record[16..18].try_into().expect("two bytes"));
            text.clear();
            use std::fmt::Write as _;
            write!(text, "{src},{dst},{edge_type};").expect("writing to a string");
            digest.update(text.as_bytes());
            let Some(count) = per_type.get_mut(usize::from(edge_type)) else {
                return Err(format!("edge record {position} has type {edge_type}, past the {} types", header.types.len()).into());
            };
            *count += 1;
            if src == dst {
                return Err(format!("edge record {position} is a self loop on {src}").into());
            }
            for endpoint in [src, dst] {
                if endpoint < spec.first || endpoint > spec.last() {
                    return Err(format!(
                        "edge record {position} ({src}->{dst}) has an endpoint outside the vertex spec's ids {}..={}",
                        spec.first,
                        spec.last()
                    )
                    .into());
                }
            }
            edges.push(EdgeEntry {
                src,
                dst,
                position,
                rank: rank_of_index[usize::from(edge_type)],
            });
        }
        let computed = format!("{:x}", digest.finalize());
        if computed != header.sha256 {
            return Err(format!(
                "{} hashes to {computed}, {} says {}",
                path.display(),
                header_path.display(),
                header.sha256
            )
            .into());
        }
        let counted = header.types.iter().cloned().zip(per_type).collect::<BTreeMap<_, _>>();
        if counted != header.edges_by_type {
            return Err(format!(
                "per-type counts {counted:?} differ from {}'s {:?}",
                header_path.display(),
                header.edges_by_type
            )
            .into());
        }
        // Sorted once, in place; any duplicate is then adjacent, whichever
        // side of the hold-back each copy is on.
        edges.sort_unstable_by_key(|edge| (edge.rank, edge.src, edge.dst));
        if let Some(pair) = edges
            .windows(2)
            .find(|pair| (pair[0].rank, pair[0].src, pair[0].dst) == (pair[1].rank, pair[1].src, pair[1].dst))
        {
            return Err(format!(
                "edge records {} and {} are both {} {}->{}",
                pair[0].position.min(pair[1].position),
                pair[0].position.max(pair[1].position),
                types[usize::from(pair[0].rank)],
                pair[0].src,
                pair[0].dst
            )
            .into());
        }
        Ok(Self { header, types, edges })
    }

    fn records(&self) -> u64 {
        self.header.edges
    }
}

/// The last `percent` of `count`, the slice a loader writes online.
fn hold_back(count: u64, percent: u64) -> u64 {
    count * percent / 100
}

/// What an image or a verification covers: vertex ids `first..end` and the
/// edges whose record position is below `edge_end`.
#[derive(Clone, Copy, Debug)]
struct Slice {
    vertex_end: u64,
    edge_end: u64,
}

impl Slice {
    fn whole(spec: &VertexSpec, edges: &EdgeList) -> Self {
        Self {
            vertex_end: spec.first + spec.count,
            edge_end: edges.records(),
        }
    }

    fn image(spec: &VertexSpec, edges: &EdgeList, percent: u64) -> Self {
        Self {
            vertex_end: spec.first + spec.count - hold_back(spec.count, percent),
            edge_end: edges.records() - hold_back(edges.records(), percent),
        }
    }

    fn has_edge(&self, edge: &EdgeEntry) -> bool {
        u64::from(edge.position) < self.edge_end
    }
}

/// One vertex family streamed in id order: the row at `position` of what
/// `vertex_rows` returns for each id.
#[derive(Clone, Debug)]
struct VertexFamily {
    prefix: String,
    position: usize,
    monotone: bool,
}

/// The vertex families an image holds, in key order. `vertex_rows` returns
/// the record, then one row per label in name order, then one per scalar
/// property in name order; the family prefix is a row's key less its id (and,
/// for a property index, its value).
fn vertex_families(encoder: &OfflineGraphImageEncoder, spec: &VertexSpec) -> Result<Vec<VertexFamily>, BoxError> {
    let rows = encoder.vertex_rows(spec.first, &spec.metadata(spec.first)?)?;
    let expected = 1 + spec.labels.len() + spec.properties.len();
    if rows.len() != expected {
        return Err(format!(
            "a vertex encodes to {} rows, {expected} expected: every property must be scalar",
            rows.len()
        )
        .into());
    }
    let generators = spec.properties.values().collect::<Vec<_>>();
    let mut families = rows
        .iter()
        .enumerate()
        .map(|(position, row)| {
            let key = std::str::from_utf8(&row.key).map_err(|_| "a vertex key is not UTF-8")?;
            let property = position.checked_sub(1 + spec.labels.len());
            let strip = if property.is_some() { 2 } else { 1 };
            let mut prefix = key;
            for _ in 0..strip {
                prefix = prefix.rsplit_once('/').ok_or("a vertex key has too few components")?.0;
            }
            Ok(VertexFamily {
                prefix: format!("{prefix}/"),
                position,
                monotone: property.is_none_or(|property| generators[property].monotone(spec.first, spec.last())),
            })
        })
        .collect::<Result<Vec<_>, BoxError>>()?;
    // Prefixes end in `/` and components cannot hold one, so no family's
    // keys interleave another's, and key order is prefix order.
    families.sort_by(|left, right| left.prefix.cmp(&right.prefix));
    Ok(families)
}

/// What the vertex workers share.
struct VertexSource {
    encoder: OfflineGraphImageEncoder,
    spec: VertexSpec,
    first: u64,
    end: u64,
    rows_per_vertex: usize,
}

impl VertexSource {
    fn rows(&self, id: u64) -> Result<Vec<OfflineImageRow>, BoxError> {
        let rows = self.encoder.vertex_rows(id, &self.spec.metadata(id)?)?;
        if rows.len() != self.rows_per_vertex {
            return Err(format!("vertex {id} encodes to {} rows, {} expected", rows.len(), self.rows_per_vertex).into());
        }
        Ok(rows)
    }

    fn chunks(&self) -> u64 {
        (self.end - self.first).div_ceil(VERTEX_CHUNK)
    }

    fn chunk(&self, chunk: u64) -> std::ops::Range<u64> {
        let start = self.first + chunk * VERTEX_CHUNK;
        start..(start + VERTEX_CHUNK).min(self.end)
    }
}

fn workers() -> usize {
    std::thread::available_parallelism().map_or(1, usize::from).saturating_sub(1).max(1)
}

/// The streamed vertex families, one after another in key order, each
/// encoded in chunks on worker threads and reassembled in id order.
struct VertexStream {
    source: Arc<VertexSource>,
    families: VecDeque<VertexFamily>,
    receivers: Vec<tokio::sync::mpsc::Receiver<Result<Vec<OfflineImageRow>, BoxError>>>,
    next_chunk: u64,
    rows: std::vec::IntoIter<OfflineImageRow>,
}

impl VertexStream {
    fn new(source: Arc<VertexSource>, families: impl IntoIterator<Item = VertexFamily>) -> Self {
        Self {
            source,
            families: families.into_iter().collect(),
            receivers: Vec::new(),
            next_chunk: 0,
            rows: Vec::new().into_iter(),
        }
    }

    fn start_family(&mut self, position: usize) {
        let workers = workers();
        self.receivers = (0..workers)
            .map(|worker| {
                let (sender, receiver) = tokio::sync::mpsc::channel(CHUNKS_AHEAD);
                let source = Arc::clone(&self.source);
                std::thread::spawn(move || {
                    let mut chunk = worker as u64;
                    while chunk < source.chunks() {
                        let rows: Result<Vec<_>, BoxError> =
                            source.chunk(chunk).map(|id| Ok(source.rows(id)?.swap_remove(position))).collect();
                        if sender.blocking_send(rows).is_err() {
                            return;
                        }
                        chunk += workers as u64;
                    }
                });
                receiver
            })
            .collect();
        self.next_chunk = 0;
    }

    async fn next(&mut self) -> Result<Option<OfflineImageRow>, BoxError> {
        loop {
            if let Some(row) = self.rows.next() {
                return Ok(Some(row));
            }
            if self.receivers.is_empty() || self.next_chunk == self.source.chunks() {
                let Some(family) = self.families.pop_front() else {
                    return Ok(None);
                };
                self.start_family(family.position);
            }
            let worker = (self.next_chunk % self.receivers.len() as u64) as usize;
            let rows = self.receivers[worker]
                .recv()
                .await
                .ok_or("a vertex worker stopped before its last chunk")??;
            self.next_chunk += 1;
            self.rows = rows.into_iter();
        }
    }
}

/// Every row of an image (or of what a store must hold) in key order: the
/// sorted rows, the edge side and any vertex family that is not monotone,
/// merged with the streamed vertex families.
struct ImageRows {
    sorted: std::iter::Peekable<SortedRows>,
    streamed: VertexStream,
    pending: Option<OfflineImageRow>,
    started: bool,
}

impl ImageRows {
    async fn next(&mut self) -> Result<Option<(Bytes, Bytes)>, BoxError> {
        if !self.started {
            self.pending = self.streamed.next().await?;
            self.started = true;
        }
        let take_sorted = match (self.sorted.peek(), &self.pending) {
            (None, None) => return Ok(None),
            (Some(Err(_)), _) => true,
            (Some(Ok((key, _))), Some(row)) if *key == row.key => {
                return Err(format!("a streamed and a sorted row share key {key:?}").into());
            }
            (Some(Ok((key, _))), Some(row)) => *key < row.key,
            (Some(_), None) => true,
            (None, Some(_)) => false,
        };
        if take_sorted {
            return Ok(Some(self.sorted.next().expect("peeked")?));
        }
        let row = self.pending.take().expect("matched a pending row");
        self.pending = self.streamed.next().await?;
        Ok(Some((row.key, row.value)))
    }
}

/// Encode a slice of the inputs: the edge side and the vertex families that
/// are not monotone into a sorter, the rest as streams. Returns the rows in
/// key order and each edge type's edges, in (src, dst) order.
fn image_rows(
    target: &Target,
    spec: &VertexSpec,
    edges: &EdgeList,
    slice: Slice,
    scratch: &Path,
) -> Result<(ImageRows, EdgesByType), BoxError> {
    let mut sorter = ExternalKvSorter::new(scratch.join("edges"), SORT_BUDGET_BYTES)?;
    let mut encoder = OfflineGraphImageEncoder::new(&target.cell_id, true)?;
    let mut by_type = BTreeMap::<String, Vec<(u64, u64)>>::new();
    for edge in edges.edges.iter().filter(|edge| slice.has_edge(edge)) {
        let edge_type = &edges.types[usize::from(edge.rank)];
        for endpoint in [edge.src, edge.dst] {
            if endpoint >= slice.vertex_end {
                return Err(format!(
                    "edge record {} ({} {}->{}) is in the image but vertex {endpoint} is held back",
                    edge.position, edge_type, edge.src, edge.dst
                )
                .into());
            }
        }
        sorter.extend(encoder.structural_edge_rows(edge_type, edge.src, edge.dst)?)?;
        by_type.entry(edge_type.clone()).or_default().push((edge.src, edge.dst));
    }
    sorter.extend(encoder.finish_rows())?;

    let vertex_encoder = OfflineGraphImageEncoder::new(&target.cell_id, true)?;
    let families = vertex_families(&vertex_encoder, spec)?;
    let source = Arc::new(VertexSource {
        encoder: vertex_encoder,
        spec: spec.clone(),
        first: spec.first,
        end: slice.vertex_end,
        rows_per_vertex: families.len(),
    });
    let (streamed, sorted): (Vec<_>, Vec<_>) = families.into_iter().partition(|family| family.monotone);
    if !sorted.is_empty() {
        let positions = sorted.iter().map(|family| family.position).collect::<Vec<_>>();
        let parts = workers();
        let ids = source.end - source.first;
        let sorters = std::thread::scope(|threads| {
            let handles = (0..parts as u64)
                .map(|part| {
                    let (source, positions) = (&source, &positions);
                    let scratch = scratch.join(format!("vertices-{part:03}"));
                    threads.spawn(move || -> Result<ExternalKvSorter, BoxError> {
                        let mut sorter = ExternalKvSorter::new(scratch, (SORT_BUDGET_BYTES / parts).max(1))?;
                        let start = source.first + ids / parts as u64 * part;
                        let end = if part + 1 == parts as u64 {
                            source.end
                        } else {
                            start + ids / parts as u64
                        };
                        for id in start..end {
                            let rows = source.rows(id)?;
                            for &position in positions {
                                sorter.push(rows[position].clone())?;
                            }
                        }
                        Ok(sorter)
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| handle.join().map_err(|_| BoxError::from("a vertex sort thread panicked"))?)
                .collect::<Result<Vec<_>, BoxError>>()
        })?;
        for part in sorters {
            sorter.absorb(part)?;
        }
    }
    let rows = ImageRows {
        sorted: sorter.into_sorted_iter()?.peekable(),
        streamed: VertexStream::new(source, streamed),
        pending: None,
        started: false,
    };
    Ok((rows, by_type))
}

/// The key family of a stored key, as `digest` and the checks name them:
/// `vertex`, `vlabel/{label}`, `vprop_idx/{property}`, `e/out`, `e/in`,
/// `cnt/out`, `cnt/in`, `seg/..`, `meta/{name}`, and otherwise the first
/// component under the cell. Keys outside the cell are `~{first component}`.
fn family_of(cell_prefix: &str, key: &[u8]) -> String {
    let key = String::from_utf8_lossy(key);
    let Some(rest) = key.strip_prefix(cell_prefix) else {
        return format!("~{}", key.split('/').next().unwrap_or_default());
    };
    let mut components = rest.split('/');
    let first = components.next().unwrap_or_default();
    match first {
        "vlabel" | "vprop_idx" | "e" | "cnt" | "seg" | "meta" => match components.next() {
            Some(second) => format!("{first}/{second}"),
            None => first.to_string(),
        },
        _ => first.to_string(),
    }
}

/// Families a structural image never holds: relationship records and their
/// indexes, and edge metadata.
const RELATIONSHIP_FAMILIES: [&str; 6] = ["rel", "rel_id", "rel_count", "rprop_idx", "rmerge_idx", "emeta"];

fn is_relationship_family(family: &str) -> bool {
    RELATIONSHIP_FAMILIES.contains(&family.split('/').next().unwrap_or_default())
}

fn decode_count(key: &[u8], value: &[u8]) -> Result<u64, BoxError> {
    Ok(u64::from_be_bytes(value.try_into().map_err(|_| {
        format!("counter {} is {} bytes, not 8", String::from_utf8_lossy(key), value.len())
    })?))
}

/// What one family of an image or a check holds.
#[derive(Clone, Debug, Default, Serialize)]
struct FamilyCount {
    rows: u64,
    /// The sum of the values, for counter families.
    #[serde(skip_serializing_if = "Option::is_none")]
    total: Option<u64>,
}

/// Counts every family of a row stream, and checks it against the slice of
/// the inputs it encodes.
#[derive(Default)]
struct FamilyCounts(BTreeMap<String, FamilyCount>);

impl FamilyCounts {
    fn add(&mut self, cell_prefix: &str, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        let family = family_of(cell_prefix, key);
        let counter = family.starts_with("cnt/");
        let entry = self.0.entry(family).or_default();
        entry.rows += 1;
        if counter {
            *entry.total.get_or_insert(0) += decode_count(key, value)?;
        }
        Ok(())
    }

    /// Every vertex family has a row per vertex, `e/out` and `e/in` a row per
    /// edge, the degree counters sum to the edges, and `meta/matrix_dirty`
    /// has a row per edge type; nothing else is present.
    fn check(&self, spec: &VertexSpec, families: &[String], vertices: u64, edges: &EdgesByType) -> Result<(), BoxError> {
        let edge_count = edges.values().map(|edges| edges.len() as u64).sum::<u64>();
        let mut expected = families.iter().map(|family| (family.clone(), vertices)).collect::<BTreeMap<_, _>>();
        if edge_count > 0 {
            expected.insert("e/out".to_string(), edge_count);
            expected.insert("e/in".to_string(), edge_count);
            expected.insert("meta/matrix_dirty".to_string(), edges.len() as u64);
        }
        let mut problems = Vec::new();
        for (family, count) in &self.0 {
            if family.starts_with("cnt/") {
                if count.total != Some(edge_count) {
                    problems.push(format!("{family} counts {:?} edges, not {edge_count}", count.total));
                }
                continue;
            }
            match expected.remove(family) {
                Some(rows) if rows == count.rows => {}
                Some(rows) => problems.push(format!("{family} has {} rows, not {rows}", count.rows)),
                None => problems.push(format!("{family} ({} rows) is not a family of this image", count.rows)),
            }
        }
        for direction in ["cnt/out", "cnt/in"] {
            if edge_count > 0 && !self.0.contains_key(direction) {
                problems.push(format!("{direction} is missing"));
            }
        }
        problems.extend(expected.keys().map(|family| format!("{family} is missing")));
        if problems.is_empty() {
            Ok(())
        } else {
            Err(format!("the image's families do not match {} vertices: {}", spec.count, problems.join("; ")).into())
        }
    }
}

fn family_names(cell_prefix: &str, families: &[VertexFamily]) -> Vec<String> {
    families
        .iter()
        .map(|family| family_of(cell_prefix, family.prefix.as_bytes()))
        .collect()
}

#[derive(Debug, Serialize)]
struct Range {
    first: u64,
    /// `first - 1` for an empty range, so a loader's own split compares
    /// equal whatever the count.
    last: i128,
}

impl Range {
    /// The last `count` of `..end`.
    fn tail(end: u64, count: u64) -> Self {
        Self {
            first: end - count,
            last: i128::from(end) - 1,
        }
    }
}

#[derive(Debug, Serialize)]
struct HeldBack {
    /// Vertex ids left out of the image.
    vertices: Range,
    /// Positions of the `edges.bin` records left out, from 0.
    edge_records: Range,
}

#[derive(Debug, Serialize)]
struct WritePath {
    cypher_engine: &'static str,
    vertices: &'static str,
    edges: &'static str,
}

#[derive(Debug, Serialize)]
struct GenerationSummary {
    generation: String,
    base_sequence: u64,
    edge_count: u64,
    checksum: u64,
}

impl From<&GraphIndexGeneration> for GenerationSummary {
    fn from(generation: &GraphIndexGeneration) -> Self {
        Self {
            generation: generation.generation.clone(),
            base_sequence: generation.base_sequence,
            edge_count: generation.edge_count,
            checksum: generation.checksum,
        }
    }
}

#[derive(Debug, Serialize)]
struct ImageSummary {
    vertices: u64,
    edges: u64,
    edges_by_type: BTreeMap<String, u64>,
    entries: u64,
    ssts: usize,
    sst_target_bytes: usize,
    families: BTreeMap<String, FamilyCount>,
}

#[derive(Debug, Serialize)]
struct Timings {
    edges_ms: u128,
    image_ms: u128,
    finalize_ms: u128,
    total_ms: u128,
}

/// `_offline_imports/graph-image/receipt.json`: what was built, from what,
/// and what was held back for a loader.
#[derive(Debug, Serialize)]
struct Receipt {
    magic: &'static str,
    tool_version: &'static str,
    database: String,
    scope: String,
    shard_path: String,
    cell_id: String,
    write_path: WritePath,
    vertex_spec: VertexSpec,
    edge_list: EdgeListHeader,
    hold_back_percent: u64,
    held_back: HeldBack,
    image: ImageSummary,
    generations: BTreeMap<String, GenerationSummary>,
    statistics_limits: StatisticsLimits,
    planner_statistics_published: bool,
    timings: Timings,
}

#[derive(Debug, Serialize)]
struct StatisticsLimits {
    max_entries: u64,
    exact_entries: u64,
}

async fn prefix_has_data(store: &Arc<dyn ObjectStore>, prefix: &str) -> Result<bool, slatedb::object_store::Error> {
    use futures::StreamExt as _;
    Ok(store.list(Some(&ObjectPath::from(prefix))).next().await.transpose()?.is_some())
}

fn scratch(parent: Option<&PathBuf>) -> Result<tempfile::TempDir, BoxError> {
    let builder = || {
        let mut builder = tempfile::Builder::new();
        builder.prefix("graph-image-");
        builder
    };
    Ok(match parent {
        Some(parent) => {
            std::fs::create_dir_all(parent)?;
            builder().tempdir_in(parent)?
        }
        None => builder().tempdir()?,
    })
}

async fn build(
    args: &BuildArgs,
    target: &Target,
    statistics: PlannerStatisticsBuildLimits,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Receipt, BoxError> {
    let started = Instant::now();
    let shard_path = target.shard_path();
    // An image starts on an empty prefix and never resumes: a failed build is
    // rebuilt on a fresh one.
    if prefix_has_data(&object_store, &shard_path).await? {
        return Err(format!("the destination {shard_path} already holds objects").into());
    }
    match object_store.head(&target.receipt_path()).await {
        Ok(_) => return Err(format!("a graph-image receipt already exists at {}", target.receipt_path()).into()),
        Err(slatedb::object_store::Error::NotFound { .. }) => {}
        Err(error) => return Err(error.into()),
    }
    let spec = VertexSpec::read(&args.inputs.vertex_spec)?;
    let edges = EdgeList::read(&args.inputs.edges, &spec)?;
    let slice = Slice::image(&spec, &edges, args.hold_back_percent);
    let scratch = scratch(args.scratch_dir.as_ref())?;
    let (mut rows, by_type) = image_rows(target, &spec, &edges, slice, scratch.path())?;
    let edges_ms = started.elapsed().as_millis();
    eprintln!(
        "graph_image_edges records={} image_edges={} elapsed_ms={edges_ms}",
        edges.records(),
        by_type.values().map(Vec::len).sum::<usize>()
    );

    let image_started = Instant::now();
    let mut builder = OfflineImageBuilder::create(
        shard_path.as_str(),
        Arc::clone(&object_store),
        OfflineImageOptions {
            target_sst_size_bytes: args.sst_target_bytes,
            compression_codec: hydradb::GRAPH_SST_COMPRESSION_CODEC,
            filter_policies: hydradb::graph_filter_policies(),
        },
    )
    .await?;
    let cell_prefix = target.cell_prefix();
    let mut counts = FamilyCounts::default();
    while let Some((key, value)) = rows.next().await? {
        counts.add(&cell_prefix, &key, &value)?;
        builder.add(key, value).await?;
    }
    drop(rows);
    let vertices = slice.vertex_end - spec.first;
    let vertex_encoder = OfflineGraphImageEncoder::new(&target.cell_id, true)?;
    let families = family_names(&cell_prefix, &vertex_families(&vertex_encoder, &spec)?);
    counts.check(&spec, &families, vertices, &by_type)?;
    let published = builder.finish().await?;
    let image_ms = image_started.elapsed().as_millis();
    eprintln!(
        "graph_image_published entries={} ssts={} elapsed_ms={image_ms} rows_per_second={}",
        published.entries,
        published.ssts,
        u128::from(published.entries) * 1000 / image_ms.max(1)
    );

    let finalize_started = Instant::now();
    let directory = ObjectStoreGraphScopeDirectory::new(
        target.base_path.clone(),
        target.root.namespace.clone(),
        target.root.graph_id.clone(),
        Arc::clone(&object_store),
    );
    let image_edges = by_type.values().map(Vec::len).sum::<usize>();
    let finalized = finalize_image(
        ImageFinalize {
            object_store: Arc::clone(&object_store),
            shard_path: shard_path.clone(),
            cell_id: &target.cell_id,
            scope: &target.scope,
            scope_label: target.scope.to_string(),
            directory: &directory,
            edges: &by_type,
            limits: offline_import_limits(usize::try_from(vertices)?, image_edges, FINALIZE_BATCH_SIZE)?,
            max_concurrent_artifact_builds: workers().clamp(1, 4),
            statistics_limits: statistics,
            verify: true,
            error_key: format!("graph-image/{shard_path}"),
        },
        |reports| check_reports(&by_type, reports),
    )
    .await?;
    for (edge_type, generation) in &finalized.generations {
        let expected = by_type.get(edge_type).map_or(0, Vec::len) as u64;
        if generation.edge_count != expected {
            return Err(format!(
                "the {edge_type} generation holds {} edges, the image {expected}",
                generation.edge_count
            )
            .into());
        }
    }
    let finalize_ms = finalize_started.elapsed().as_millis();
    eprintln!("graph_image_finalized elapsed_ms={finalize_ms}");

    let receipt = Receipt {
        magic: RECEIPT_MAGIC,
        tool_version: env!("CARGO_PKG_VERSION"),
        database: target.database.clone(),
        scope: target.scope.to_string(),
        shard_path,
        cell_id: target.cell_id.clone(),
        write_path: WritePath {
            cypher_engine: MIRRORED_CYPHER_ENGINE,
            vertices: "UpsertVertices (merge_vertex_metadata_batch)",
            edges: "CreateEdges (write_edge_mutations_batch)",
        },
        vertex_spec: spec.clone(),
        edge_list: edges.header.clone(),
        hold_back_percent: args.hold_back_percent,
        held_back: HeldBack {
            vertices: Range::tail(spec.first + spec.count, hold_back(spec.count, args.hold_back_percent)),
            edge_records: Range::tail(edges.records(), hold_back(edges.records(), args.hold_back_percent)),
        },
        image: ImageSummary {
            vertices,
            edges: image_edges as u64,
            edges_by_type: by_type
                .iter()
                .map(|(edge_type, edges)| (edge_type.clone(), edges.len() as u64))
                .collect(),
            entries: published.entries,
            ssts: published.ssts,
            sst_target_bytes: args.sst_target_bytes,
            families: counts.0,
        },
        generations: finalized
            .generations
            .iter()
            .map(|(edge_type, generation)| (edge_type.clone(), generation.into()))
            .collect(),
        statistics_limits: StatisticsLimits {
            max_entries: statistics.max_entries,
            exact_entries: statistics.exact_entries,
        },
        planner_statistics_published: finalized.planner_statistics_published,
        timings: Timings {
            edges_ms,
            image_ms,
            finalize_ms,
            total_ms: started.elapsed().as_millis(),
        },
    };
    object_store
        .put_opts(
            &target.receipt_path(),
            Bytes::from(serde_json::to_vec_pretty(&receipt)?).into(),
            PutMode::Create.into(),
        )
        .await?;
    Ok(receipt)
}

/// Every type's verification saw exactly the image's edges of it.
fn check_reports(by_type: &EdgesByType, reports: &[VerifiedEdgeType]) -> hydradb::Result<()> {
    for (edge_type, edges, report) in reports {
        let edges = *edges as u64;
        if report.canonical_edges != edges || report.out_index_edges != edges || report.in_index_edges != edges {
            return Err(GraphError::CorruptValue {
                key: format!("graph-image/{edge_type}"),
                reason: format!(
                    "verified {} canonical, {} out and {} in edges for an image of {edges}",
                    report.canonical_edges, report.out_index_edges, report.in_index_edges
                ),
            });
        }
        if report.relationship_records != 0 || report.relationship_merge_indexes != 0 {
            return Err(GraphError::CorruptValue {
                key: format!("graph-image/{edge_type}"),
                reason: "a structural image holds relationship records".to_string(),
            });
        }
    }
    if reports.len() != by_type.len() {
        return Err(GraphError::CorruptValue {
            key: "graph-image".to_string(),
            reason: format!("verified {} edge types of {}", reports.len(), by_type.len()),
        });
    }
    Ok(())
}

/// Open a store read-only, as the indexer's cached readers do: no writer, no
/// checkpoint, no background manifest poll.
async fn open_read_only(target: &Target, object_store: Arc<dyn ObjectStore>) -> hydradb::Result<GraphShard> {
    GraphShard::open_with_options(
        target.shard_path(),
        object_store,
        GraphOpenOptions::follow_latest_reader(
            GraphLimits::default().max_wal_tail_files,
            GraphOpenOptions::default().cache.slatedb_cache_bytes,
        ),
    )
    .await
}

#[derive(Debug, Serialize)]
struct VerifyReport {
    shard_path: String,
    storage_sequence: Option<u64>,
    /// Rows compared equal, by family.
    compared: BTreeMap<String, u64>,
    /// Rows of families this check does not cover (receipts, the changelog,
    /// cursors, write-backs), by family.
    not_compared: BTreeMap<String, u64>,
}

/// Compare every vertex and edge family of the store with the rows the whole
/// spec and edge list produce: what a store holds once any held-back slice
/// has been loaded.
async fn verify(args: &VerifyArgs, target: &Target, object_store: Arc<dyn ObjectStore>) -> Result<VerifyReport, BoxError> {
    let spec = VertexSpec::read(&args.inputs.vertex_spec)?;
    let edges = EdgeList::read(&args.inputs.edges, &spec)?;
    let scratch = scratch(args.scratch_dir.as_ref())?;
    let (mut expected, _) = image_rows(target, &spec, &edges, Slice::whole(&spec, &edges), scratch.path())?;
    let shard = open_read_only(target, object_store).await?;
    let result = async {
        let snapshot = shard.snapshot(&target.cell_id).await?;
        let cell_prefix = target.cell_prefix();
        let mut stored = snapshot.scan_storage_prefix(cell_prefix.as_bytes()).await?;
        let mut report = VerifyReport {
            shard_path: target.shard_path(),
            storage_sequence: snapshot.storage_sequence(),
            compared: BTreeMap::new(),
            not_compared: BTreeMap::new(),
        };
        let compared = |family: &str| {
            matches!(family, "vertex" | "meta/matrix_dirty")
                || ["vlabel/", "vprop_idx/", "e/", "cnt/"]
                    .iter()
                    .any(|prefix| family.starts_with(prefix))
        };
        let mut next_expected = expected.next().await?;
        while let Some(row) = stored.next().await? {
            let family = family_of(&cell_prefix, &row.key);
            if is_relationship_family(&family) {
                return Err(format!("{} is a relationship key in a structural store", String::from_utf8_lossy(&row.key)).into());
            }
            if !compared(&family) {
                *report.not_compared.entry(family).or_default() += 1;
                continue;
            }
            let Some((key, value)) = next_expected.take() else {
                return Err(format!("the store holds {} past every expected row", String::from_utf8_lossy(&row.key)).into());
            };
            if key != row.key {
                let (missing, extra) = (String::from_utf8_lossy(&key), String::from_utf8_lossy(&row.key));
                return Err(if key < row.key {
                    format!("the store lacks {missing}")
                } else {
                    format!("the store holds {extra}, which the inputs do not")
                }
                .into());
            }
            // An edge type's dirty marker holds a sequence, which differs
            // between any two stores; its presence is what the inputs fix.
            if family != "meta/matrix_dirty" && value != row.value {
                return Err(format!("{} differs from the inputs' value", String::from_utf8_lossy(&key)).into());
            }
            *report.compared.entry(family).or_default() += 1;
            next_expected = expected.next().await?;
        }
        if let Some((key, _)) = next_expected {
            return Err(format!("the store lacks {}", String::from_utf8_lossy(&key)).into());
        }
        Ok::<_, BoxError>(report)
    }
    .await;
    let close = shard.close().await;
    let report = result?;
    close?;
    Ok(report)
}

#[derive(Debug, Default, Serialize)]
struct FamilyDigest {
    count: u64,
    /// sha256 over each key, length-prefixed, in key order.
    keys_sha256: String,
    /// sha256 over each key and value, length-prefixed, in key order.
    sha256: String,
}

#[derive(Default)]
struct FamilyHasher {
    count: u64,
    keys: Sha256,
    rows: Sha256,
}

impl FamilyHasher {
    fn add(&mut self, key: &[u8], value: &[u8]) {
        self.count += 1;
        let length = |bytes: &[u8]| (bytes.len() as u64).to_be_bytes();
        self.keys.update(length(key));
        self.keys.update(key);
        self.rows.update(length(key));
        self.rows.update(key);
        self.rows.update(length(value));
        self.rows.update(value);
    }

    fn finish(self) -> FamilyDigest {
        FamilyDigest {
            count: self.count,
            keys_sha256: format!("{:x}", self.keys.finalize()),
            sha256: format!("{:x}", self.rows.finalize()),
        }
    }
}

#[derive(Debug, Serialize)]
struct SortedRunShape {
    id: u32,
    ssts: usize,
    bytes: u64,
}

#[derive(Debug, Default, Serialize)]
struct ObjectsShape {
    count: u64,
    bytes: u64,
}

#[derive(Debug, Serialize)]
struct StoreShape {
    /// Newest first, as the manifest lists them.
    sorted_runs: Vec<SortedRunShape>,
    compacted_ssts: usize,
    l0_ssts: usize,
    /// Objects under the shard path by their first path component.
    objects: BTreeMap<String, ObjectsShape>,
}

#[derive(Debug, Serialize)]
struct StatisticsHeader {
    storage_sequence: u64,
    built_at_ms: u64,
    format_version: u32,
}

impl From<&PlannerStatistics> for StatisticsHeader {
    fn from(statistics: &PlannerStatistics) -> Self {
        Self {
            storage_sequence: statistics.storage_sequence,
            built_at_ms: statistics.built_at_ms,
            format_version: statistics.format_version,
        }
    }
}

#[derive(Debug, Serialize)]
struct CountedStatistics {
    storage_sequence: u64,
    /// sha256 of the encoded object with its sequence and build time zeroed:
    /// equal for two stores whose counted keys are equal.
    sha256: String,
    truncated: bool,
    labels: BTreeMap<String, u64>,
    edge_types: BTreeMap<String, u64>,
    vertex_properties: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DigestReport {
    shard_path: String,
    storage_sequence: Option<u64>,
    families: BTreeMap<String, FamilyDigest>,
    max_xlog_sequence: Option<u64>,
    generations: BTreeMap<String, Option<GenerationSummary>>,
    shape: StoreShape,
    /// The header of the statistics object the store ships, if any.
    statistics: Option<StatisticsHeader>,
    counted_statistics: CountedStatistics,
}

#[derive(Debug, Serialize)]
struct StatisticsHeaderReport {
    statistics: Option<StatisticsHeader>,
    max_xlog_sequence: Option<u64>,
}

/// The sequence of an `xlog/{type}/{sequence}/{src}/{dst}` key under the cell.
fn xlog_sequence(cell_prefix: &str, key: &[u8]) -> Option<u64> {
    let key = std::str::from_utf8(key).ok()?;
    let mut components = key.strip_prefix(cell_prefix)?.strip_prefix("xlog/")?.split('/');
    components.next()?;
    components.next()?.parse().ok()
}

async fn statistics_header(target: &Target, object_store: Arc<dyn ObjectStore>) -> Result<StatisticsHeaderReport, BoxError> {
    let shard = open_read_only(target, object_store).await?;
    let result = async {
        let snapshot = shard.snapshot(&target.cell_id).await?;
        let cell_prefix = target.cell_prefix();
        let mut xlog = snapshot.scan_storage_prefix(format!("{cell_prefix}xlog/").as_bytes()).await?;
        let mut max_xlog_sequence = None;
        while let Some(row) = xlog.next().await? {
            max_xlog_sequence = max_xlog_sequence.max(xlog_sequence(&cell_prefix, &row.key));
        }
        Ok::<_, BoxError>(StatisticsHeaderReport {
            statistics: shard.read_planner_statistics(&target.cell_id).await?.as_ref().map(Into::into),
            max_xlog_sequence,
        })
    }
    .await;
    let close = shard.close().await;
    let report = result?;
    close?;
    Ok(report)
}

async fn digest(
    target: &Target,
    statistics_limits: PlannerStatisticsBuildLimits,
    object_store: Arc<dyn ObjectStore>,
) -> Result<DigestReport, BoxError> {
    let shard_path = target.shard_path();
    let shape = store_shape(&shard_path, Arc::clone(&object_store)).await?;
    let shard = open_read_only(target, object_store).await?;
    let result = async {
        let snapshot = shard.snapshot(&target.cell_id).await?;
        let cell_prefix = target.cell_prefix();
        let mut rows = snapshot.scan_storage_prefix(b"").await?;
        let mut families = BTreeMap::<String, FamilyHasher>::new();
        let mut max_xlog_sequence = None;
        let mut edge_types = BTreeSet::new();
        let dirty_prefix = format!("{cell_prefix}meta/matrix_dirty/");
        while let Some(row) = rows.next().await? {
            let family = family_of(&cell_prefix, &row.key);
            if family == "xlog" {
                max_xlog_sequence = max_xlog_sequence.max(xlog_sequence(&cell_prefix, &row.key));
            }
            if let Some(edge_type) = row.key.strip_prefix(dirty_prefix.as_bytes()) {
                edge_types.insert(String::from_utf8_lossy(edge_type).into_owned());
            }
            families.entry(family).or_default().add(&row.key, &row.value);
        }
        let storage_sequence = snapshot.storage_sequence();
        drop(rows);
        let mut generations = BTreeMap::new();
        for edge_type in edge_types {
            let generation = shard.current_graph_index(&target.cell_id, &edge_type).await?;
            generations.insert(edge_type, generation.as_ref().map(Into::into));
        }
        let mut counted = shard.build_planner_statistics_with(&target.cell_id, &statistics_limits).await?;
        let counted_sequence = counted.storage_sequence;
        counted.storage_sequence = 0;
        counted.built_at_ms = 0;
        Ok::<_, BoxError>(DigestReport {
            shard_path: shard_path.clone(),
            storage_sequence,
            families: families.into_iter().map(|(family, hasher)| (family, hasher.finish())).collect(),
            max_xlog_sequence,
            generations,
            shape,
            statistics: shard.read_planner_statistics(&target.cell_id).await?.as_ref().map(Into::into),
            counted_statistics: CountedStatistics {
                storage_sequence: counted_sequence,
                sha256: format!("{:x}", Sha256::digest(encode_planner_statistics(&counted))),
                truncated: counted.truncated,
                labels: counted.labels.clone(),
                edge_types: counted.edge_types.clone(),
                vertex_properties: counted.vertex_properties.keys().cloned().collect(),
            },
        })
    }
    .await;
    let close = shard.close().await;
    let report = result?;
    close?;
    Ok(report)
}

/// The store's sorted runs and L0s from its manifest, read through SlateDB's
/// admin reader, and its objects by kind from a listing. Both only read.
async fn store_shape(shard_path: &str, object_store: Arc<dyn ObjectStore>) -> Result<StoreShape, BoxError> {
    use futures::TryStreamExt as _;

    let manifest = slatedb::admin::Admin::builder(shard_path, Arc::clone(&object_store))
        .build()
        .read_manifest(None)
        .await?
        .ok_or_else(|| format!("no SlateDB manifest under {shard_path}"))?;
    let sorted_runs = manifest
        .compacted()
        .iter()
        .map(|run| SortedRunShape {
            id: run.id,
            ssts: run.sst_views().len(),
            bytes: run.estimate_size(),
        })
        .collect::<Vec<_>>();
    let mut objects = BTreeMap::<String, ObjectsShape>::new();
    let prefix = ObjectPath::from(shard_path);
    let mut listing = object_store.list(Some(&prefix));
    while let Some(object) = listing.try_next().await? {
        let kind = object
            .location
            .prefix_match(&prefix)
            .and_then(|mut parts| parts.next())
            .map_or_else(String::new, |part| part.as_ref().to_string());
        let entry = objects.entry(kind).or_default();
        entry.count += 1;
        entry.bytes += object.size;
    }
    Ok(StoreShape {
        compacted_ssts: sorted_runs.iter().map(|run| run.ssts).sum(),
        sorted_runs,
        l0_ssts: manifest.l0().len(),
        objects,
    })
}
