#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

use hydradb_pg_graph_core::GraphError;
use hydradb_pg_graph_query as kernels;
use hydradb_pg_graph_query::{native_paths, Checkpoint, PathConfig, PathKind, PathQuery, QueryConfig};
use pgrx::{pg_sys, prelude::*, JsonB};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};

mod adjacency;
mod keys;
mod spi;
mod storage;
mod usage;

use adjacency::AdjacencyGraph;
use usage::Function;

pgrx::pg_module_magic!();

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    usage::init();
}

/// The per-call settings a SQL function parses from its options object.
pub(crate) trait CallConfig: DeserializeOwned {
    fn check(&self) -> Result<(), GraphError>;
    fn max_result_bytes(&self) -> usize;
}

impl CallConfig for QueryConfig {
    fn check(&self) -> Result<(), GraphError> {
        self.validate()
    }

    fn max_result_bytes(&self) -> usize {
        self.max_result_bytes
    }
}

impl CallConfig for PathConfig {
    // Path options are validated against the procedure kind by `native_paths`.
    fn check(&self) -> Result<(), GraphError> {
        Ok(())
    }

    fn max_result_bytes(&self) -> usize {
        self.max_result_bytes
    }
}

pub(crate) struct Options<C> {
    config: C,
    /// Fail rather than return a result cut short by anything but the
    /// caller's own depth, vertex and edge limits.
    strict: bool,
    max_snapshot_bytes: usize,
}

impl<C: CallConfig> Options<C> {
    fn parse(mut value: Value) -> Result<Self, GraphError> {
        let object = value
            .as_object_mut()
            .ok_or_else(|| GraphError::Source("options must be a JSON object".into()))?;
        let mut limit = |key: &str, default: usize, maximum: usize| -> Result<usize, GraphError> {
            let value = match object.remove(key) {
                None => default,
                Some(value) => value
                    .as_u64()
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or_else(|| GraphError::Source(format!("{key} must be a positive integer")))?,
            };
            if value == 0 || value > maximum {
                return Err(GraphError::Source(format!("{key} must be between 1 and {maximum}")));
            }
            Ok(value)
        };
        let max_snapshot_bytes = limit("max_snapshot_bytes", 128 * 1024 * 1024, 512 * 1024 * 1024)?;
        let strict = match object.remove("strict") {
            None => false,
            Some(Value::Bool(strict)) => strict,
            Some(_) => return Err(GraphError::Source("strict must be true or false".into())),
        };
        let config: C = serde_json::from_value(value).map_err(|error| GraphError::Source(error.to_string()))?;
        config.check()?;
        Ok(Self {
            config,
            strict,
            max_snapshot_bytes,
        })
    }
}

/// Truncation reasons that are the caller's own `max_depth`, `max_vertices`
/// and `max_edges`; `strict` fails on any other.
const CALLER_LIMITS: [&str; 3] = ["depth", "vertices", "edges"];

struct PostgresCheckpoint;

impl Checkpoint for PostgresCheckpoint {
    fn is_cancelled(&mut self) -> bool {
        pg_sys::check_for_interrupts!();
        false
    }
}

fn run<C: CallConfig>(
    function: Function,
    graph_name: &str,
    options: JsonB,
    prepare: impl FnOnce(&C) -> Result<(), GraphError>,
    query: impl FnOnce(&mut AdjacencyGraph, &C, &mut PostgresCheckpoint) -> Result<Value, GraphError>,
) -> JsonB {
    let mut call = usage::Call::start(function);
    let execute = || -> Result<JsonB, GraphError> {
        let invalid = |error| {
            usage::fail("invalid_options");
            error
        };
        let options = Options::<C>::parse(options.0).map_err(invalid)?;
        // Before the graph is opened, so an invalid request reads nothing.
        prepare(&options.config).map_err(invalid)?;
        let mut graph = storage::open_graph(graph_name, &options)?;
        let mut result = query(&mut graph, &options.config, &mut PostgresCheckpoint)?;
        if options.strict {
            let unrequested = result["truncated"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .filter(|reason| !CALLER_LIMITS.contains(reason))
                .collect::<Vec<_>>();
            if !unrequested.is_empty() {
                usage::fail("strict");
                return Err(GraphError::Source(format!(
                    "strict: the query stopped before the caller's limits: {}",
                    unrequested.join(", ")
                )));
            }
        }
        result["storage"] = json!({
            "vertices": graph.loaded_vertices(),
            "edges": graph.loaded_edges(),
            "estimated_bytes": graph.loaded_bytes(),
            "lookups": graph.lookups(),
        });
        if serde_json::to_vec(&result)
            .map_err(|error| GraphError::Source(error.to_string()))?
            .len()
            > options.config.max_result_bytes()
        {
            usage::fail("max_result_bytes");
            return Err(GraphError::Source("serialized response exceeds max_result_bytes".into()));
        }
        call.succeeded(&result, graph.loaded_vertices(), graph.loaded_edges());
        Ok(JsonB(result))
    };
    execute().unwrap_or_else(|error| {
        match error {
            GraphError::MissingVertexKey { .. } => usage::fail_unless_set("missing_seed"),
            GraphError::LimitExceeded { .. } => usage::fail_unless_set("limit_exceeded"),
            GraphError::InvalidInput(_) | GraphError::InvalidConfig(_) => usage::fail_unless_set("invalid_options"),
            _ => {}
        }
        pgrx::error!("{}", error)
    })
}

/// The bytes of `max_result_bytes` a response has left. Each vertex or edge
/// name is charged as the result is built, so a result with long keys stops at
/// the budget before it is built, not once it is.
struct ResultBudget(usize);

impl ResultBudget {
    /// Charges no more than the name serializes to, so a result that fits is
    /// never refused: text counts its quotes and not its escapes, an integer
    /// its digits and sign.
    fn charge(&mut self, value: Value) -> Result<Value, GraphError> {
        let size = |field: &Value| match field {
            Value::String(text) => text.len() + 2,
            Value::Number(number) => number.as_i64().map_or(1, |n| {
                n.unsigned_abs().checked_ilog10().map_or(1, |digits| digits as usize + 1) + usize::from(n < 0)
            }),
            _ => 4,
        };
        // `{"label":` `,"key":` `}`.
        let size = size(&value["label"]) + size(&value["key"]) + 17;
        self.0 = self.0.checked_sub(size).ok_or_else(|| {
            usage::fail("max_result_bytes");
            GraphError::Source("serialized response exceeds max_result_bytes".into())
        })?;
        Ok(value)
    }
}

fn to_json(value: impl Serialize) -> Result<Value, GraphError> {
    serde_json::to_value(value).map_err(|error| GraphError::Source(error.to_string()))
}

/// The seeds' vertices; a seed that names no vertex is an error.
fn existing(graph: &mut AdjacencyGraph, seeds: &Value) -> Result<Vec<i64>, GraphError> {
    graph
        .seeds(seeds)?
        .into_iter()
        .map(|seed| match seed.id {
            Some(id) => Ok(id),
            None => Err(GraphError::MissingVertexKey {
                label: seed.label,
                key: seed.key.to_string(),
            }),
        })
        .collect()
}

/// Every function takes seeds as `{"label": key}` or `{"label": [key, ...]}`
/// and names vertices and edges in its result as `{"label", "key"}`.
#[pg_extern(stable, parallel_unsafe)]
fn reachable_vertices(graph_name: &str, seeds: JsonB, options: JsonB) -> JsonB {
    run::<QueryConfig>(
        Function::ReachableVertices,
        graph_name,
        options,
        |_| Ok(()),
        |graph, config, checkpoint| {
            let seeds = existing(graph, &seeds.0)?;
            let result = kernels::reachable(graph, &seeds, config, checkpoint)?;
            graph.finish()?;
            let mut budget = ResultBudget(config.max_result_bytes);
            let items = result
                .items
                .iter()
                .map(|item| Ok(with_depth(budget.charge(graph.vertex_name(item.vertex_id))?, item.depth)))
                .collect::<Result<_, GraphError>>()?;
            keyed(items, &result.truncated, &result.stats)
        },
    )
}

#[pg_extern(stable, parallel_unsafe)]
fn reachable_edges(graph_name: &str, seeds: JsonB, options: JsonB) -> JsonB {
    run::<QueryConfig>(
        Function::ReachableEdges,
        graph_name,
        options,
        |_| Ok(()),
        |graph, config, checkpoint| {
            let seeds = existing(graph, &seeds.0)?;
            let result = kernels::nearby_edges(graph, &seeds, config, checkpoint)?;
            graph.finish()?;
            let mut budget = ResultBudget(config.max_result_bytes);
            let items = result
                .items
                .iter()
                .map(|item| Ok(with_depth(budget.charge(graph.edge_name(item.edge_id))?, item.depth)))
                .collect::<Result<_, GraphError>>()?;
            keyed(items, &result.truncated, &result.stats)
        },
    )
}

/// `{"label", "key"}` plus the depth it was reached at.
fn with_depth(mut item: Value, depth: u32) -> Value {
    item["depth"] = depth.into();
    item
}

fn keyed(items: Vec<Value>, truncated: impl Serialize, stats: impl Serialize) -> Result<Value, GraphError> {
    Ok(json!({ "items": items, "truncated": to_json(truncated)?, "stats": to_json(stats)? }))
}

/// Simple paths, shortest first, from `sources` to `targets` or to any vertex.
/// The graph is unweighted. A seed the graph does not have is an error.
fn paths(function: Function, graph_name: &str, options: JsonB, sources: &Value, targets: Option<&Value>) -> JsonB {
    run::<PathConfig>(
        function,
        graph_name,
        options,
        |config| config.validate(PathKind::MultiSource),
        |graph, config, checkpoint| {
            let sources = existing(graph, sources)?;
            let targets = targets.map(|targets| existing(graph, targets)).transpose()?;
            // The path search compares vertex ids, so it runs on a CSR of the
            // region it reads, numbered by `(label, key)`.
            graph.load_path_region(config, &sources, targets.as_deref())?;
            let region = graph.loaded_csr()?;
            let local = |ids: &[i64]| ids.iter().map(|id| region.local[id]).collect::<Vec<_>>();
            let (local_sources, local_targets) = (local(&sources), targets.as_deref().map(local));
            let query = PathQuery {
                kind: PathKind::MultiSource,
                sources: &local_sources,
                targets: local_targets.as_deref(),
            };
            let result = native_paths(&region.graph, &query, config, checkpoint)?;
            let (vertex_ids, edge_ids) = (region.vertex_ids, region.edge_ids);
            let mut result = to_json(&result)?;
            let mut budget = ResultBudget(config.max_result_bytes);
            for path in result["items"].as_array_mut().into_iter().flatten() {
                for (field, vertex) in [("vertices", true), ("edges", false)] {
                    let ids = path[field].as_array().map_or(&[][..], Vec::as_slice);
                    let names = ids
                        .iter()
                        .filter_map(Value::as_i64)
                        .map(|id| {
                            budget.charge(if vertex {
                                graph.vertex_name(vertex_ids[id as usize])
                            } else {
                                graph.edge_name(edge_ids[id as usize])
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    path[field] = names.into();
                }
            }
            Ok(result)
        },
    )
}

/// Paths from each source to any vertex: `paths_per_pair` per source.
#[pg_extern(stable, parallel_unsafe)]
fn paths_from(graph_name: &str, sources: JsonB, options: JsonB) -> JsonB {
    paths(Function::PathsFrom, graph_name, options, &sources.0, None)
}

/// Paths from each source to each target: `paths_per_pair` per pair.
#[pg_extern(stable, parallel_unsafe)]
fn paths_between(graph_name: &str, sources: JsonB, targets: JsonB, options: JsonB) -> JsonB {
    paths(Function::PathsBetween, graph_name, options, &sources.0, Some(&targets.0))
}
