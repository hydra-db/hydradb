//! The Neo4j Graph Data Science calls the `graphdatascience` Python client
//! issues for a projection and the two centrality algorithms, answered
//! natively outside both Cypher planners.
//!
//! The shapes are exactly what the client's `call_procedure` and
//! `call_function` build -- `CALL gds.<name>($a, $b) YIELD x, y` and
//! `RETURN gds.<name>($a)` -- and the same calls with literal arguments, for
//! raw Cypher. `gds.debug.arrow` answers that no Arrow endpoint runs, so the
//! client stays on Bolt. Anything else under `gds.` is refused with Neo4j's
//! own wording for an unknown procedure or function. Inside a supported call,
//! every option that would change the answer and is not implemented is
//! refused by name rather than ignored.

use std::collections::BTreeMap;

use super::path_procedure::{lex, Token};
use crate::{GraphError, QueryColumn, QueryFailureReason, Result, VertexPropertyValue};

/// The GDS release whose procedure signatures this surface answers. The
/// client compares it with the oldest server version it supports.
pub(crate) const GDS_VERSION: &str = "2.13.0";

/// Lists and maps inside one argument; a GDS projection or config needs 3.
const MAX_VALUE_NESTING: usize = 16;

const PAGE_RANK_MAX_ITERATIONS: u32 = 20;
const PAGE_RANK_DAMPING_FACTOR: f64 = 0.85;
const PAGE_RANK_TOLERANCE: f64 = 1e-7;

/// Configuration keys GDS defines for bookkeeping, progress or scheduling,
/// none of which changes a result. The client sets `jobId` on every call.
const IGNORED_CONFIGURATION: [&str; 6] = ["concurrency", "jobId", "logProgress", "sudo", "username", "readConcurrency"];

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GdsProcedure {
    pub(crate) call: GdsCall,
    /// The result columns, in order: the `YIELD` list, or every column the
    /// call has.
    pub(crate) columns: Vec<GdsColumn>,
}

impl GdsProcedure {
    pub(crate) fn columns(&self) -> Vec<QueryColumn> {
        self.columns.iter().map(|column| QueryColumn::new(column.name.clone())).collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GdsColumn {
    pub(crate) field: GdsField,
    pub(crate) name: String,
}

/// One value a GDS call can return, named as GDS names it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GdsField {
    Version,
    GraphName,
    NodeProjection,
    RelationshipProjection,
    NodeCount,
    RelationshipCount,
    ProjectMillis,
    Exists,
    Database,
    DatabaseLocation,
    Configuration,
    MemoryUsage,
    SizeInBytes,
    CreationTime,
    ModificationTime,
    SchemaWithOrientation,
    Density,
    NodeId,
    Score,
    ListenAddress,
    Enabled,
    Running,
    Versions,
    RanIterations,
    DidConverge,
    CentralityDistribution,
    PreProcessingMillis,
    ComputeMillis,
    PostProcessingMillis,
    WriteMillis,
    NodePropertiesWritten,
}

impl GdsField {
    fn name(self) -> &'static str {
        match self {
            Self::Version => "gdsVersion",
            Self::ListenAddress => "listenAddress",
            Self::Enabled => "enabled",
            Self::Running => "running",
            Self::Versions => "versions",
            Self::RanIterations => "ranIterations",
            Self::DidConverge => "didConverge",
            Self::CentralityDistribution => "centralityDistribution",
            Self::PreProcessingMillis => "preProcessingMillis",
            Self::ComputeMillis => "computeMillis",
            Self::PostProcessingMillis => "postProcessingMillis",
            Self::WriteMillis => "writeMillis",
            Self::NodePropertiesWritten => "nodePropertiesWritten",
            Self::GraphName => "graphName",
            Self::NodeProjection => "nodeProjection",
            Self::RelationshipProjection => "relationshipProjection",
            Self::NodeCount => "nodeCount",
            Self::RelationshipCount => "relationshipCount",
            Self::ProjectMillis => "projectMillis",
            Self::Exists => "exists",
            Self::Database => "database",
            Self::DatabaseLocation => "databaseLocation",
            Self::Configuration => "configuration",
            Self::MemoryUsage => "memoryUsage",
            Self::SizeInBytes => "sizeInBytes",
            Self::CreationTime => "creationTime",
            Self::ModificationTime => "modificationTime",
            Self::SchemaWithOrientation => "schemaWithOrientation",
            Self::Density => "density",
            Self::NodeId => "nodeId",
            Self::Score => "score",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GdsCall {
    Version,
    /// `gds.debug.arrow`, answered as a server with no Arrow endpoint. Client
    /// 1.x calls it on connect and fails if it is missing; with Arrow off, the
    /// client sends everything over Bolt.
    DebugArrow,
    Project(GdsProjection),
    Exists {
        graph_name: String,
    },
    Drop {
        graph_names: Vec<String>,
        fail_if_missing: bool,
    },
    PageRank {
        graph_name: String,
        config: GdsPageRankConfig,
    },
    Betweenness {
        graph_name: String,
        config: GdsBetweennessConfig,
    },
}

impl GdsCall {
    pub(crate) fn procedure_name(&self) -> &'static str {
        match self {
            Self::Version => "gds.version",
            Self::DebugArrow => "gds.debug.arrow",
            Self::Project(_) => "gds.graph.project",
            Self::Exists { .. } => "gds.graph.exists",
            Self::Drop { .. } => "gds.graph.drop",
            Self::PageRank { config, .. } if config.write_property.is_some() => "gds.pageRank.write",
            Self::PageRank { .. } => "gds.pageRank.stream",
            Self::Betweenness { config, .. } if config.write_property.is_some() => "gds.betweenness.write",
            Self::Betweenness { .. } => "gds.betweenness.stream",
        }
    }

    /// The property a `.write` call stores its scores in.
    pub(crate) fn write_property(&self) -> Option<&str> {
        match self {
            Self::PageRank { config, .. } => config.write_property.as_deref(),
            Self::Betweenness { config, .. } => config.write_property.as_deref(),
            _ => None,
        }
    }

    fn fields(&self) -> &'static [GdsField] {
        use GdsField::*;
        match self {
            Self::Version => &[Version],
            Self::DebugArrow => &[ListenAddress, Enabled, Running, Versions],
            Self::Project(_) => &[
                GraphName,
                NodeProjection,
                RelationshipProjection,
                NodeCount,
                RelationshipCount,
                ProjectMillis,
            ],
            Self::Exists { .. } => &[GraphName, Exists],
            Self::Drop { .. } => &[
                GraphName,
                Database,
                DatabaseLocation,
                Configuration,
                MemoryUsage,
                SizeInBytes,
                NodeCount,
                RelationshipCount,
                CreationTime,
                ModificationTime,
                SchemaWithOrientation,
                Density,
            ],
            Self::PageRank { config, .. } if config.write_property.is_some() => &[
                RanIterations,
                DidConverge,
                CentralityDistribution,
                PreProcessingMillis,
                ComputeMillis,
                PostProcessingMillis,
                WriteMillis,
                NodePropertiesWritten,
                Configuration,
            ],
            Self::Betweenness { config, .. } if config.write_property.is_some() => &[
                CentralityDistribution,
                PreProcessingMillis,
                ComputeMillis,
                PostProcessingMillis,
                WriteMillis,
                NodePropertiesWritten,
                Configuration,
            ],
            Self::PageRank { .. } | Self::Betweenness { .. } => &[NodeId, Score],
        }
    }
}

/// What `gds.graph.project` selects: vertices by label, relationships by type
/// and orientation, read from the published index generations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GdsProjection {
    pub(crate) graph_name: String,
    pub(crate) nodes: GdsNodeProjection,
    pub(crate) relationships: Vec<GdsRelationshipProjection>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GdsNodeProjection {
    /// `'*'`: every endpoint of a projected relationship.
    All,
    /// Vertices carrying any of these labels, sorted and distinct.
    Labels(Vec<String>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GdsRelationshipProjection {
    /// `None` is `'*'`: every relationship type in the cell.
    pub(crate) edge_type: Option<String>,
    pub(crate) orientation: GdsOrientation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum GdsOrientation {
    Natural,
    Reverse,
    Undirected,
}

impl GdsOrientation {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Natural => "NATURAL",
            Self::Reverse => "REVERSE",
            Self::Undirected => "UNDIRECTED",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_uppercase().as_str() {
            "NATURAL" => Some(Self::Natural),
            "REVERSE" => Some(Self::Reverse),
            "UNDIRECTED" => Some(Self::Undirected),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GdsPageRankConfig {
    pub(crate) max_iterations: u32,
    pub(crate) damping_factor: f64,
    pub(crate) tolerance: f64,
    /// The `.write` mode's `writeProperty`; `None` is `.stream`.
    pub(crate) write_property: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GdsBetweennessConfig {
    /// Source vertices to sample; `None` samples
    /// [`DEFAULT_BETWEENNESS_SAMPLING_SIZE`], and a size at or above the
    /// vertex count uses every vertex, which is exact.
    pub(crate) sampling_size: Option<u64>,
    pub(crate) sampling_seed: Option<i64>,
    /// The `.write` mode's `writeProperty`; `None` is `.stream`.
    pub(crate) write_property: Option<String>,
}

/// Sources betweenness samples when the call names no `samplingSize`.
/// Exact betweenness costs one breadth-first search per vertex, which no
/// deadline bounds on a large graph; 512 sources keep the estimate's error
/// small on the graphs it is run on while the cost stays flat.
pub(crate) const DEFAULT_BETWEENNESS_SAMPLING_SIZE: u64 = 512;
/// The seed of the default sample, so repeated calls agree.
pub(crate) const DEFAULT_BETWEENNESS_SAMPLING_SEED: i64 = 0;

impl GdsBetweennessConfig {
    /// The sample a call on `vertices` vertices draws: `(size, seed)`, with
    /// no seed meaning a fresh one per call.
    pub(crate) fn effective_sampling(&self, vertices: u64) -> (u64, Option<i64>) {
        match self.sampling_size {
            Some(size) => (size.min(vertices), self.sampling_seed),
            None => (
                DEFAULT_BETWEENNESS_SAMPLING_SIZE.min(vertices),
                Some(self.sampling_seed.unwrap_or(DEFAULT_BETWEENNESS_SAMPLING_SEED)),
            ),
        }
    }
}

/// Whether the query is a `CALL gds.*` or `RETURN gds.*(...)` call. GDS
/// namespaces are case-sensitive; the keyword is not.
pub(crate) fn is_gds_call(query: &str) -> bool {
    let query = query.trim_start();
    ["CALL", "RETURN"].into_iter().any(|keyword| {
        query
            .get(..keyword.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
            && query[keyword.len()..].starts_with(|ch: char| ch.is_ascii_whitespace())
            && query[keyword.len()..].trim_start().starts_with("gds.")
    })
}

/// Whether a [`is_gds_call`] query writes: `gds.graph.project` and
/// `gds.graph.drop` keep the projection record on the cell's writer.
///
/// The name is read with the lexer and name rule the parser executes, so a
/// spelling such as ``gds.graph.`drop` `` that runs a drop also classifies as
/// one. Arguments are not parsed: they may name parameters classification
/// does not have.
pub(crate) fn is_gds_catalog_write(query: &str) -> bool {
    let Ok(tokens) = lex(query) else {
        return false;
    };
    let mut parser = Parser {
        tokens,
        offset: 0,
        parameters: &|_| None,
    };
    parser.keyword("CALL")
        && parser.qualified_name().is_ok_and(|name| {
            matches!(
                name.as_str(),
                "gds.graph.project" | "gds.graph.drop" | "gds.pageRank.write" | "gds.betweenness.write"
            )
        })
}

/// A literal or parameter argument, as GDS reads it.
#[derive(Clone, Debug, PartialEq)]
enum GdsValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    List(Vec<GdsValue>),
    Map(BTreeMap<String, GdsValue>),
}

impl GdsValue {
    fn kind(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "a boolean",
            Self::Integer(_) => "an integer",
            Self::Float(_) => "a float",
            Self::String(_) => "a string",
            Self::List(_) => "a list",
            Self::Map(_) => "a map",
        }
    }
}

/// Parse a call [`is_gds_call`] accepted, resolving scalar parameters. The
/// client layer resolves composite ones with
/// [`parse_gds_procedure_with_query_parameters`] and forwards the printed
/// literal call, so the text arriving here names none.
pub(crate) fn parse_gds_procedure(query: &str, parameters: &BTreeMap<String, VertexPropertyValue>) -> Result<GdsProcedure> {
    parse_with(query, &|name| parameters.get(name).map(scalar_value))
}

/// [`parse_gds_procedure`] with the client's parameters, lists and maps
/// included.
#[cfg(feature = "client-api")]
pub(crate) fn parse_gds_procedure_with_query_parameters(
    query: &str,
    parameters: &BTreeMap<String, crate::QueryParameterValue>,
) -> Result<GdsProcedure> {
    parse_with(query, &|name| parameters.get(name).map(query_parameter_value))
}

type ParameterLookup<'a> = &'a dyn Fn(&str) -> Option<Result<GdsValue>>;

fn scalar_value(value: &VertexPropertyValue) -> Result<GdsValue> {
    Ok(match value {
        VertexPropertyValue::Integer(value) => GdsValue::Integer(
            i64::try_from(*value).map_err(|_| procedure_error(format!("parameter {value} exceeds the signed 64-bit range")))?,
        ),
        VertexPropertyValue::SignedInteger(value) => GdsValue::Integer(*value),
        VertexPropertyValue::Float(value) => GdsValue::Float(value.0),
        VertexPropertyValue::Bool(value) => GdsValue::Bool(*value),
        VertexPropertyValue::String(value) => GdsValue::String(value.clone()),
        // No gds configuration key takes a date or a typed property list, so
        // either one is a caller error rather than a value to coerce.
        VertexPropertyValue::Date(_) | VertexPropertyValue::List(_) => {
            return Err(procedure_error("gds parameters are numbers, booleans, strings, lists or maps"));
        }
    })
}

#[cfg(feature = "client-api")]
fn query_parameter_value(value: &crate::QueryParameterValue) -> Result<GdsValue> {
    use crate::QueryParameterValue;
    Ok(match value {
        QueryParameterValue::Scalar(value) => scalar_value(value)?,
        QueryParameterValue::List(values) => GdsValue::List(values.iter().map(query_parameter_value).collect::<Result<_>>()?),
        QueryParameterValue::Map(entries) => GdsValue::Map(
            entries
                .iter()
                .map(|(key, value)| Ok((key.clone(), query_parameter_value(value)?)))
                .collect::<Result<_>>()?,
        ),
    })
}

fn parse_with(query: &str, parameters: ParameterLookup<'_>) -> Result<GdsProcedure> {
    let mut parser = Parser {
        tokens: lex(query)?,
        offset: 0,
        parameters,
    };
    if parser.keyword("RETURN") {
        let name = parser.qualified_name()?;
        let arguments = parser.arguments()?;
        let alias = if parser.keyword("AS") { Some(parser.identifier()?) } else { None };
        parser.end()?;
        let call = match name.as_str() {
            "gds.version" => {
                arity(&name, &arguments, 0, 0)?;
                GdsCall::Version
            }
            "gds.graph.exists" => {
                arity(&name, &arguments, 1, 1)?;
                GdsCall::Exists {
                    graph_name: graph_name(&arguments[0])?,
                }
            }
            _ => return unsupported(format!("Unknown function '{name}'")),
        };
        // Neo4j names an unaliased column after the expression as written.
        let name = alias.unwrap_or_else(|| return_expression_text(query));
        let field = if call == GdsCall::Version {
            GdsField::Version
        } else {
            GdsField::Exists
        };
        return Ok(GdsProcedure {
            call,
            columns: vec![GdsColumn { field, name }],
        });
    }
    if !parser.keyword("CALL") {
        return parse_error("expected CALL or RETURN");
    }
    let name = parser.qualified_name()?;
    let arguments = parser.arguments()?;
    let call = match name.as_str() {
        "gds.debug.arrow" => {
            arity(&name, &arguments, 0, 0)?;
            GdsCall::DebugArrow
        }
        "gds.graph.project" => project_call(&name, &arguments)?,
        "gds.graph.exists" => {
            arity(&name, &arguments, 1, 1)?;
            GdsCall::Exists {
                graph_name: graph_name(&arguments[0])?,
            }
        }
        "gds.graph.drop" => drop_call(&name, &arguments)?,
        "gds.pageRank.stream" | "gds.pageRank.write" => {
            arity(&name, &arguments, 1, 2)?;
            GdsCall::PageRank {
                graph_name: graph_name(&arguments[0])?,
                config: page_rank_config(&name, arguments.get(1))?,
            }
        }
        "gds.betweenness.stream" | "gds.betweenness.write" => {
            arity(&name, &arguments, 1, 2)?;
            GdsCall::Betweenness {
                graph_name: graph_name(&arguments[0])?,
                config: betweenness_config(&name, arguments.get(1))?,
            }
        }
        _ => {
            return unsupported(format!(
                "There is no procedure with the name `{name}` registered for this database instance. \
                 Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed."
            ))
        }
    };
    let columns = if parser.keyword("YIELD") {
        parser.yields(&name, call.fields())?
    } else {
        call.fields()
            .iter()
            .map(|field| GdsColumn {
                field: *field,
                name: field.name().to_string(),
            })
            .collect()
    };
    parser.end()?;
    Ok(GdsProcedure { call, columns })
}

fn project_call(name: &str, arguments: &[GdsValue]) -> Result<GdsCall> {
    arity(name, arguments, 3, 4)?;
    let graph_name = graph_name(&arguments[0])?;
    let nodes = node_projection(&arguments[1])?;
    let relationships = relationship_projection(&arguments[2])?;
    if let Some(config) = arguments.get(3) {
        let config = configuration(name, config)?;
        for (key, value) in config {
            match key.as_str() {
                key if IGNORED_CONFIGURATION.contains(&key) => {}
                "nodeProperties" | "relationshipProperties" if is_empty(value) => {}
                "nodeProperties" | "relationshipProperties" => {
                    return unsupported(format!("{name} {key} are not supported; only the graph structure is projected"))
                }
                key => return unsupported(format!("{name} configuration key {key} is not supported")),
            }
        }
    }
    Ok(GdsCall::Project(GdsProjection {
        graph_name,
        nodes,
        relationships,
    }))
}

fn drop_call(name: &str, arguments: &[GdsValue]) -> Result<GdsCall> {
    arity(name, arguments, 1, 4)?;
    let graph_names = match &arguments[0] {
        GdsValue::List(values) => values.iter().map(graph_name).collect::<Result<Vec<_>>>()?,
        value => vec![graph_name(value)?],
    };
    let fail_if_missing = match arguments.get(1) {
        None | Some(GdsValue::Null) => true,
        Some(GdsValue::Bool(value)) => *value,
        Some(value) => return unsupported(format!("{name} failIfMissing must be a boolean, not {}", value.kind())),
    };
    // Positional `dbName` and `username`: the client sends empty strings for
    // "the current database" and "the calling user".
    for (position, argument) in ["dbName", "username"].into_iter().zip(arguments.iter().skip(2)) {
        if !matches!(argument, GdsValue::Null) && *argument != GdsValue::String(String::new()) {
            return unsupported(format!(
                "{name} {position} is not supported; graphs belong to the cell they were projected from"
            ));
        }
    }
    Ok(GdsCall::Drop {
        graph_names,
        fail_if_missing,
    })
}

fn node_projection(value: &GdsValue) -> Result<GdsNodeProjection> {
    let mut labels = Vec::new();
    match value {
        GdsValue::String(label) => labels.push(label.clone()),
        GdsValue::List(values) => {
            for value in values {
                match value {
                    GdsValue::String(label) => labels.push(label.clone()),
                    value => return unsupported(format!("a node projection list holds labels, not {}", value.kind())),
                }
            }
        }
        GdsValue::Map(entries) => {
            for (key, entry) in entries {
                match entry {
                    GdsValue::String(label) => labels.push(label.clone()),
                    GdsValue::Map(fields) => {
                        let mut label = key.clone();
                        for (field, value) in fields {
                            match (field.as_str(), value) {
                                ("label", GdsValue::String(value)) => label = value.clone(),
                                ("properties", value) if is_empty(value) => {}
                                ("properties", _) => {
                                    return unsupported("node properties are not supported; only the graph structure is projected")
                                }
                                (field, _) => return unsupported(format!("node projection key {field} is not supported")),
                            }
                        }
                        labels.push(label);
                    }
                    value => return unsupported(format!("node projection {key} must be a label or a map, not {}", value.kind())),
                }
            }
        }
        GdsValue::Null => return unsupported("Cypher projections are not supported; project by label"),
        value => return unsupported(format!("a node projection is a label, a list or a map, not {}", value.kind())),
    }
    if labels.is_empty() {
        return unsupported("a node projection needs at least one label or '*'");
    }
    if labels.iter().any(|label| label == "*") {
        return Ok(GdsNodeProjection::All);
    }
    for label in &labels {
        crate::validate_component("label", label)?;
    }
    labels.sort();
    labels.dedup();
    Ok(GdsNodeProjection::Labels(labels))
}

fn relationship_projection(value: &GdsValue) -> Result<Vec<GdsRelationshipProjection>> {
    let mut projections = Vec::new();
    match value {
        GdsValue::String(edge_type) => projections.push(natural(edge_type)),
        GdsValue::List(values) => {
            for value in values {
                match value {
                    GdsValue::String(edge_type) => projections.push(natural(edge_type)),
                    value => return unsupported(format!("a relationship projection list holds types, not {}", value.kind())),
                }
            }
        }
        GdsValue::Map(entries) => {
            for (key, entry) in entries {
                match entry {
                    GdsValue::String(edge_type) => projections.push(natural(edge_type)),
                    GdsValue::Map(fields) => {
                        let mut edge_type = key.clone();
                        let mut orientation = GdsOrientation::Natural;
                        for (field, value) in fields {
                            match (field.as_str(), value) {
                                ("type", GdsValue::String(value)) => edge_type = value.clone(),
                                ("orientation", GdsValue::String(value)) => {
                                    orientation = GdsOrientation::parse(value).ok_or_else(|| GraphError::UnsupportedQuery {
                                        reason: QueryFailureReason::Procedure,
                                        dialect: "OpenCypher",
                                        feature: format!("orientation {value} is not NATURAL, REVERSE or UNDIRECTED"),
                                    })?;
                                }
                                // An index over incoming relationships changes
                                // what GDS keeps in memory, not any answer.
                                ("indexInverse", GdsValue::Bool(_)) => {}
                                ("properties", value) if is_empty(value) => {}
                                ("properties", _) => {
                                    return unsupported("relationship properties are not supported; only the graph structure is projected")
                                }
                                ("aggregation", _) => {
                                    return unsupported(
                                        "relationship aggregation is not supported; parallel relationships always collapse to one",
                                    )
                                }
                                (field, _) => return unsupported(format!("relationship projection key {field} is not supported")),
                            }
                        }
                        projections.push(GdsRelationshipProjection {
                            edge_type: (edge_type != "*").then_some(edge_type),
                            orientation,
                        });
                    }
                    value => {
                        return unsupported(format!(
                            "relationship projection {key} must be a type or a map, not {}",
                            value.kind()
                        ))
                    }
                }
            }
        }
        GdsValue::Null => return unsupported("Cypher projections are not supported; project by relationship type"),
        value => {
            return unsupported(format!(
                "a relationship projection is a type, a list or a map, not {}",
                value.kind()
            ))
        }
    }
    if projections.is_empty() {
        return unsupported("a relationship projection needs at least one type or '*'");
    }
    for edge_type in projections.iter().filter_map(|projection| projection.edge_type.as_ref()) {
        crate::validate_component("edge_type", edge_type)?;
    }
    projections.sort_by(|left, right| (&left.edge_type, left.orientation).cmp(&(&right.edge_type, right.orientation)));
    projections.dedup();
    Ok(projections)
}

fn natural(edge_type: &str) -> GdsRelationshipProjection {
    GdsRelationshipProjection {
        edge_type: (edge_type != "*").then(|| edge_type.to_string()),
        orientation: GdsOrientation::Natural,
    }
}

fn page_rank_config(name: &str, config: Option<&GdsValue>) -> Result<GdsPageRankConfig> {
    let mut result = GdsPageRankConfig {
        max_iterations: PAGE_RANK_MAX_ITERATIONS,
        damping_factor: PAGE_RANK_DAMPING_FACTOR,
        tolerance: PAGE_RANK_TOLERANCE,
        write_property: None,
    };
    let entries = match config {
        Some(config) => configuration(name, config)?.iter().collect(),
        None => Vec::new(),
    };
    for (key, value) in entries {
        if write_option(name, key, value, &mut result.write_property)? {
            continue;
        }
        match key.as_str() {
            "maxIterations" => {
                result.max_iterations = u32::try_from(integer(name, key, value)?)
                    .ok()
                    .filter(|value| *value >= 1)
                    .ok_or_else(|| procedure_error(format!("{name} maxIterations must be at least 1")))?;
            }
            "dampingFactor" => {
                result.damping_factor = number(name, key, value)?;
                if !(0.0..1.0).contains(&result.damping_factor) {
                    return unsupported(format!("{name} dampingFactor must be in [0, 1)"));
                }
            }
            "tolerance" => {
                result.tolerance = number(name, key, value)?;
                if result.tolerance < 0.0 {
                    return unsupported(format!("{name} tolerance must not be negative"));
                }
            }
            "scaler" if is_no_scaler(value) => {}
            "scaler" => return unsupported(format!("{name} scaler is not supported; scores are returned unscaled")),
            "sourceNodes" if *value == GdsValue::Null => {}
            "sourceNodes" => {
                return unsupported(format!(
                    "{name} sourceNodes is not supported; personalised PageRank is not implemented"
                ))
            }
            key => algorithm_option(name, key, value)?,
        }
    }
    require_write_property(name, &result.write_property)?;
    Ok(result)
}

fn betweenness_config(name: &str, config: Option<&GdsValue>) -> Result<GdsBetweennessConfig> {
    let mut result = GdsBetweennessConfig {
        sampling_size: None,
        sampling_seed: None,
        write_property: None,
    };
    let entries = match config {
        Some(config) => configuration(name, config)?.iter().collect(),
        None => Vec::new(),
    };
    for (key, value) in entries {
        if write_option(name, key, value, &mut result.write_property)? {
            continue;
        }
        match (key.as_str(), value) {
            ("samplingSize" | "samplingSeed", GdsValue::Null) => {}
            ("samplingSize", value) => {
                result.sampling_size = Some(
                    u64::try_from(integer(name, key, value)?)
                        .ok()
                        .filter(|value| *value >= 1)
                        .ok_or_else(|| procedure_error(format!("{name} samplingSize must be at least 1")))?,
                );
            }
            ("samplingSeed", value) => result.sampling_seed = Some(integer(name, key, value)?),
            (key, value) => algorithm_option(name, key, value)?,
        }
    }
    require_write_property(name, &result.write_property)?;
    Ok(result)
}

/// The `.write` mode's own keys, taken only by a `.write` call: whether
/// `key` was one of them.
fn write_option(name: &str, key: &str, value: &GdsValue, write_property: &mut Option<String>) -> Result<bool> {
    if !name.ends_with(".write") {
        return Ok(false);
    }
    match (key, value) {
        // `id` is the vertex's identity, not one of its properties, so SET
        // refuses it as well.
        ("writeProperty", GdsValue::String(property)) if property.eq_ignore_ascii_case("id") => {
            unsupported(format!("{name} writeProperty cannot be {property}, the node's identity"))
        }
        ("writeProperty", GdsValue::String(property)) => {
            crate::validate_component("property", property)?;
            *write_property = Some(property.clone());
            Ok(true)
        }
        ("writeProperty", value) => unsupported(format!("{name} writeProperty must be a string, not {}", value.kind())),
        // How many threads GDS writes with; the write-back here is chunked
        // transactions on the cell's writer.
        ("writeConcurrency", _) => Ok(true),
        _ => Ok(false),
    }
}

fn require_write_property(name: &str, write_property: &Option<String>) -> Result<()> {
    if name.ends_with(".write") && write_property.is_none() {
        return unsupported(format!("{name} requires writeProperty"));
    }
    Ok(())
}

/// The options both algorithms share: bookkeeping that is ignored, and the
/// filters and weights that are refused.
fn algorithm_option(name: &str, key: &str, value: &GdsValue) -> Result<()> {
    match key {
        key if IGNORED_CONFIGURATION.contains(&key) => Ok(()),
        // The client always sends `['*']`: run on the whole projection.
        "nodeLabels" | "relationshipTypes" if is_all_filter(value) => Ok(()),
        "nodeLabels" | "relationshipTypes" => unsupported(format!(
            "{name} {key} filters are not supported; project a graph with only those instead"
        )),
        "relationshipWeightProperty" if *value == GdsValue::Null => Ok(()),
        "relationshipWeightProperty" => unsupported(format!("{name} relationshipWeightProperty is not supported; graphs are unweighted")),
        key => unsupported(format!("{name} configuration key {key} is not supported")),
    }
}

fn is_all_filter(value: &GdsValue) -> bool {
    match value {
        GdsValue::String(value) => value == "*",
        GdsValue::List(values) => values.iter().all(|value| *value == GdsValue::String("*".to_string())),
        _ => false,
    }
}

fn is_no_scaler(value: &GdsValue) -> bool {
    match value {
        GdsValue::Null => true,
        GdsValue::String(value) => value.eq_ignore_ascii_case("none"),
        GdsValue::Map(fields) => matches!(fields.get("type"), Some(GdsValue::String(value)) if value.eq_ignore_ascii_case("none")),
        _ => false,
    }
}

fn is_empty(value: &GdsValue) -> bool {
    match value {
        GdsValue::Null => true,
        GdsValue::List(values) => values.is_empty(),
        GdsValue::Map(entries) => entries.is_empty(),
        GdsValue::String(value) => value.is_empty(),
        _ => false,
    }
}

fn configuration<'a>(name: &str, value: &'a GdsValue) -> Result<&'a BTreeMap<String, GdsValue>> {
    match value {
        GdsValue::Map(entries) => Ok(entries),
        value => unsupported(format!("{name} configuration must be a map, not {}", value.kind())),
    }
}

fn integer(name: &str, key: &str, value: &GdsValue) -> Result<i64> {
    match value {
        GdsValue::Integer(value) => Ok(*value),
        value => unsupported(format!("{name} {key} must be an integer, not {}", value.kind())),
    }
}

fn number(name: &str, key: &str, value: &GdsValue) -> Result<f64> {
    match value {
        GdsValue::Integer(value) => Ok(*value as f64),
        GdsValue::Float(value) => Ok(*value),
        value => unsupported(format!("{name} {key} must be a number, not {}", value.kind())),
    }
}

fn graph_name(value: &GdsValue) -> Result<String> {
    match value {
        GdsValue::String(name) => {
            crate::validate_component("graph_name", name)?;
            Ok(name.clone())
        }
        value => unsupported(format!("a graph name must be a string, not {}", value.kind())),
    }
}

fn arity(name: &str, arguments: &[GdsValue], min: usize, max: usize) -> Result<()> {
    if (min..=max).contains(&arguments.len()) {
        return Ok(());
    }
    let expected = if min == max { min.to_string() } else { format!("{min} to {max}") };
    unsupported(format!("{name} takes {expected} arguments, not {}", arguments.len()))
}

/// The `RETURN` expression as written, which is how Neo4j names an
/// unaliased column.
fn return_expression_text(query: &str) -> String {
    let query = query.trim();
    let query = query.strip_suffix(';').unwrap_or(query).trim_end();
    query["RETURN".len()..].trim_start().to_string()
}

struct Parser<'a> {
    tokens: Vec<Token>,
    offset: usize,
    parameters: ParameterLookup<'a>,
}

impl Parser<'_> {
    fn keyword(&mut self, expected: &str) -> bool {
        match self.tokens.get(self.offset) {
            Some(Token::Identifier(actual)) if actual.eq_ignore_ascii_case(expected) => {
                self.offset += 1;
                true
            }
            _ => false,
        }
    }

    fn qualified_name(&mut self) -> Result<String> {
        let mut name = self.identifier()?;
        while self.optional(&Token::Dot) {
            name.push('.');
            name.push_str(&self.identifier()?);
        }
        Ok(name)
    }

    fn arguments(&mut self) -> Result<Vec<GdsValue>> {
        self.token(&Token::LeftParen)?;
        let mut arguments = Vec::new();
        if self.optional(&Token::RightParen) {
            return Ok(arguments);
        }
        loop {
            arguments.push(self.value()?);
            if self.optional(&Token::RightParen) {
                return Ok(arguments);
            }
            self.token(&Token::Comma)?;
        }
    }

    fn value(&mut self) -> Result<GdsValue> {
        self.nested_value(0)
    }

    /// One value, `depth` lists or maps deep. The depth is bounded so a
    /// query of nested brackets is a parse error, not a stack overflow.
    fn nested_value(&mut self, depth: usize) -> Result<GdsValue> {
        if depth > MAX_VALUE_NESTING {
            return parse_error(format!("values nest more than {MAX_VALUE_NESTING} deep"));
        }
        match self.next()? {
            Token::Identifier(value) if value.eq_ignore_ascii_case("null") => Ok(GdsValue::Null),
            Token::Identifier(value) if value.eq_ignore_ascii_case("true") => Ok(GdsValue::Bool(true)),
            Token::Identifier(value) if value.eq_ignore_ascii_case("false") => Ok(GdsValue::Bool(false)),
            Token::String(value) => Ok(GdsValue::String(value)),
            Token::Integer(value) => Ok(GdsValue::Integer(value)),
            Token::Float(value) => Ok(GdsValue::Float(value)),
            Token::Parameter(name) => (self.parameters)(&name).unwrap_or(Err(GraphError::MissingQueryParameter {
                dialect: "OpenCypher",
                name,
            })),
            Token::LeftBracket => {
                let mut values = Vec::new();
                if self.optional(&Token::RightBracket) {
                    return Ok(GdsValue::List(values));
                }
                loop {
                    values.push(self.nested_value(depth + 1)?);
                    if self.optional(&Token::RightBracket) {
                        return Ok(GdsValue::List(values));
                    }
                    self.token(&Token::Comma)?;
                }
            }
            Token::LeftBrace => {
                let mut entries = BTreeMap::new();
                if self.optional(&Token::RightBrace) {
                    return Ok(GdsValue::Map(entries));
                }
                loop {
                    let key = match self.next()? {
                        Token::Identifier(key) | Token::QuotedIdentifier(key) | Token::String(key) => key,
                        token => return parse_error(format!("expected a map key, found {token:?}")),
                    };
                    self.token(&Token::Colon)?;
                    let value = self.nested_value(depth + 1)?;
                    if entries.insert(key.clone(), value).is_some() {
                        return parse_error(format!("duplicate map key {key}"));
                    }
                    if self.optional(&Token::RightBrace) {
                        return Ok(GdsValue::Map(entries));
                    }
                    self.token(&Token::Comma)?;
                }
            }
            token => parse_error(format!("unexpected value {token:?}")),
        }
    }

    /// `YIELD a, b AS c`: each a column `fields` has, at most once.
    fn yields(&mut self, name: &str, fields: &[GdsField]) -> Result<Vec<GdsColumn>> {
        let mut columns = Vec::<GdsColumn>::new();
        loop {
            let yielded = self.identifier()?;
            let Some(field) = fields.iter().copied().find(|field| field.name() == yielded) else {
                return parse_error(format!("{name} does not yield {yielded}"));
            };
            let column = if self.keyword("AS") { self.identifier()? } else { yielded };
            if columns.iter().any(|existing| existing.field == field || existing.name == column) {
                return parse_error(format!("{name} yields {column} more than once"));
            }
            columns.push(GdsColumn { field, name: column });
            if !self.optional(&Token::Comma) {
                return Ok(columns);
            }
        }
    }

    /// A name, plain or in backticks. Only a plain one is ever a keyword.
    fn identifier(&mut self) -> Result<String> {
        match self.next()? {
            Token::Identifier(value) | Token::QuotedIdentifier(value) => Ok(value),
            token => parse_error(format!("expected identifier, found {token:?}")),
        }
    }

    fn token(&mut self, expected: &Token) -> Result<()> {
        let actual = self.next()?;
        if actual == *expected {
            Ok(())
        } else {
            parse_error(format!("expected {expected:?}, found {actual:?}"))
        }
    }

    fn optional(&mut self, expected: &Token) -> bool {
        if self.tokens.get(self.offset) == Some(expected) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<Token> {
        let token = self.tokens.get(self.offset).cloned().ok_or_else(|| GraphError::QueryParse {
            dialect: "OpenCypher",
            reason: "unexpected end of query".to_string(),
        })?;
        self.offset += 1;
        Ok(token)
    }

    fn end(&mut self) -> Result<()> {
        self.optional(&Token::Semicolon);
        if self.offset == self.tokens.len() {
            Ok(())
        } else {
            parse_error("unexpected tokens after a gds call; only YIELD may follow it")
        }
    }
}

fn procedure_error(feature: impl Into<String>) -> GraphError {
    GraphError::UnsupportedQuery {
        reason: QueryFailureReason::Procedure,
        dialect: "OpenCypher",
        feature: feature.into(),
    }
}

fn unsupported<T>(feature: impl Into<String>) -> Result<T> {
    Err(procedure_error(feature))
}

fn parse_error<T>(reason: impl Into<String>) -> Result<T> {
    Err(GraphError::QueryParse {
        dialect: "OpenCypher",
        reason: reason.into(),
    })
}

impl GdsProcedure {
    /// The call as literal text that parses back to `self`: every argument
    /// written out, every column named in `YIELD` or `AS`. The one printer for
    /// a parsed call, so its quoting rules live only here and in the lexer.
    #[cfg_attr(not(feature = "client-api"), allow(dead_code))]
    pub(crate) fn to_cypher(&self) -> String {
        let arguments = match &self.call {
            GdsCall::Version | GdsCall::DebugArrow => String::new(),
            GdsCall::Exists { graph_name } => quote_string(graph_name),
            GdsCall::Project(projection) => {
                let nodes = match &projection.nodes {
                    GdsNodeProjection::All => quote_string("*"),
                    GdsNodeProjection::Labels(labels) => list(labels.iter().map(|label| quote_string(label))),
                };
                // Keyed by position: a type may be projected in two
                // orientations, and the parser reads the type from `type`.
                let relationships = map(projection.relationships.iter().enumerate().map(|(index, relationship)| {
                    let edge_type = relationship.edge_type.as_deref().unwrap_or("*");
                    (
                        format!("r{index}"),
                        map([
                            ("type".to_string(), quote_string(edge_type)),
                            ("orientation".to_string(), quote_string(relationship.orientation.as_str())),
                        ]),
                    )
                }));
                format!("{}, {nodes}, {relationships}", quote_string(&projection.graph_name))
            }
            GdsCall::Drop {
                graph_names,
                fail_if_missing,
            } => format!("{}, {fail_if_missing}", list(graph_names.iter().map(|name| quote_string(name)))),
            GdsCall::PageRank { graph_name, config } => {
                let mut entries = vec![
                    ("maxIterations".to_string(), config.max_iterations.to_string()),
                    ("dampingFactor".to_string(), float(config.damping_factor)),
                    ("tolerance".to_string(), float(config.tolerance)),
                ];
                if let Some(property) = &config.write_property {
                    entries.push(("writeProperty".to_string(), quote_string(property)));
                }
                format!("{}, {}", quote_string(graph_name), map(entries))
            }
            GdsCall::Betweenness { graph_name, config } => {
                let mut entries = Vec::new();
                if let Some(property) = &config.write_property {
                    entries.push(("writeProperty".to_string(), quote_string(property)));
                }
                if let Some(size) = config.sampling_size {
                    entries.push(("samplingSize".to_string(), size.to_string()));
                }
                if let Some(seed) = config.sampling_seed {
                    entries.push(("samplingSeed".to_string(), seed.to_string()));
                }
                format!("{}, {}", quote_string(graph_name), map(entries))
            }
        };
        let name = self.call.procedure_name();
        let is_function = matches!(self.call, GdsCall::Version)
            || matches!(self.call, GdsCall::Exists { .. }) && self.columns.len() == 1 && self.columns[0].field == GdsField::Exists;
        if is_function {
            return format!("RETURN {name}({arguments}) AS {}", quote_name(&self.columns[0].name));
        }
        let yields = self
            .columns
            .iter()
            .map(|column| format!("{} AS {}", column.field.name(), quote_name(&column.name)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("CALL {name}({arguments}) YIELD {yields}")
    }
}

fn quote_string(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');
    for ch in value.chars() {
        if ch == '\\' || ch == '\'' {
            quoted.push('\\');
        }
        quoted.push(ch);
    }
    quoted.push('\'');
    quoted
}

/// Backticks around every name, so none can read as a keyword.
fn quote_name(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Display, which never uses an exponent the lexer cannot read and is the
/// shortest text that parses back to the same value; with a fraction always,
/// so it does not read back as an integer.
fn float(value: f64) -> String {
    let text = value.to_string();
    if text.contains('.') {
        text
    } else {
        format!("{text}.0")
    }
}

fn list(values: impl IntoIterator<Item = String>) -> String {
    format!("[{}]", values.into_iter().collect::<Vec<_>>().join(", "))
}

fn map(entries: impl IntoIterator<Item = (String, String)>) -> String {
    let entries = entries
        .into_iter()
        .map(|(key, value)| format!("{}: {value}", quote_name(&key)))
        .collect::<Vec<_>>();
    format!("{{{}}}", entries.join(", "))
}
