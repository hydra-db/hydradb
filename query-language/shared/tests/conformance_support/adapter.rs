use async_trait::async_trait;
use hydradb::{
    prepare_opencypher_query_with_parameters, validate_experimental_cypher_preparation, validate_opencypher_syntax, CypherEngineMode,
    GraphError, GraphInspection, GraphShard, PreparedOpenCypherQuery, QueryColumn, QueryContext, QueryFloat, QueryOutput, QueryPath,
    QueryPathNode, QueryPathRelationship, QueryResultSet, QueryRow, QueryValue, VertexPropertyValue,
};
use hydradb_cypher_ast::{
    BinaryOperator, Clause, Expression, Literal, PathPattern, PatternElement, ProjectionItems, Statement, UnaryOperator,
};
use hydradb_cypher_parser_antlr::parse_cypher25_syntax;
use hydradb_cypher_tck_harness::{
    lifecycle::{Adapter, AdapterFailure, Diagnostic, GraphState, QueryObservation, QueryOutcome},
    steps::Kind,
    values::{Direction, Node, Path, Relationship, Value},
};
use serde::Deserialize;
use slatedb::object_store::memory::InMemory;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tempfile::TempDir;
use tokio::time::{timeout, Duration};

const CELL_ID: &str = "tck";
const QUERY_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_SAFE_LITERAL_NESTING: usize = 32;

pub struct HydraAdapter {
    mode: CypherEngineMode,
    corpus_root: PathBuf,
}

pub struct HydraSession {
    shard: GraphShard,
    _directory: TempDir,
    scalar_parameters: BTreeMap<String, VertexPropertyValue>,
    list_parameters: BTreeMap<String, Vec<VertexPropertyValue>>,
    request_sequence: u64,
}

impl HydraAdapter {
    pub fn new(mode: CypherEngineMode, corpus_root: impl Into<PathBuf>) -> Self {
        Self {
            mode,
            corpus_root: corpus_root.into(),
        }
    }

    fn execution_path(&self, query: &str, session: &HydraSession) -> &'static str {
        if self.mode == CypherEngineMode::Experimental {
            if matches!(
                prepare_opencypher_query_with_parameters(query, &session.scalar_parameters, &session.list_parameters),
                Ok(PreparedOpenCypherQuery::Write(_))
            ) {
                return "experimental mode; shared transactional mutation dispatcher";
            }
            return "experimental Cypher 25 read engine";
        }
        "legacy libcypher parser and shard executor"
    }

    async fn execute_internal(&self, session: &mut HydraSession, query: &str) -> Result<QueryOutput, GraphError> {
        session.request_sequence += 1;
        let context = QueryContext::new(CELL_ID, format!("tck-{}-{}", self.mode.as_str(), session.request_sequence))
            .with_cypher_engine(self.mode)
            .with_timeout_ms(1_500)
            .with_parameters(session.scalar_parameters.clone())
            .with_list_parameters(session.list_parameters.clone());
        if matches!(
            prepare_opencypher_query_with_parameters(query, &session.scalar_parameters, &session.list_parameters),
            Ok(PreparedOpenCypherQuery::Write(_))
        ) {
            session.shard.execute_cypher(context, query).await
        } else {
            session.shard.execute_cypher_rows(context, query).await.map(QueryOutput::Rows)
        }
    }

    async fn provision_fixture(&self, session: &mut HydraSession, fixture: &Kind) -> Result<(), AdapterFailure> {
        let Kind::NamedGraph(name) = fixture else {
            // `any graph` deliberately chooses the empty graph. Official TCK
            // scenarios using it must be valid for every graph state.
            return Ok(());
        };
        let metadata_path = self.corpus_root.join("graphs").join(name).join(format!("{name}.json"));
        let metadata: NamedGraph = serde_json::from_slice(
            &std::fs::read(&metadata_path)
                .map_err(|error| AdapterFailure::Fixture(format!("read {}: {error}", metadata_path.display())))?,
        )
        .map_err(|error| AdapterFailure::Fixture(format!("parse {}: {error}", metadata_path.display())))?;
        if metadata.name != *name {
            return Err(AdapterFailure::Fixture(format!(
                "named graph metadata {} declares {:?}",
                metadata_path.display(),
                metadata.name
            )));
        }
        let graph_directory = metadata_path.parent().expect("metadata path has parent");
        for script in metadata.scripts {
            let script_path = named_graph_script(graph_directory, &script);
            let source = std::fs::read_to_string(&script_path)
                .map_err(|error| AdapterFailure::Fixture(format!("read {}: {error}", script_path.display())))?;
            for statement in source.split(';').map(str::trim).filter(|statement| !statement.is_empty()) {
                timeout(QUERY_TIMEOUT, self.execute_internal(session, statement))
                    .await
                    .map_err(|_| AdapterFailure::Timeout)?
                    .map_err(|error| graph_operation_failure("fixture", Some(&script_path), error))?;
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct NamedGraph {
    name: String,
    scripts: Vec<String>,
}

#[async_trait]
impl Adapter for HydraAdapter {
    type Session = HydraSession;

    async fn create(&self) -> Result<Self::Session, AdapterFailure> {
        let directory = tempfile::tempdir().map_err(|error| AdapterFailure::Infrastructure(error.to_string()))?;
        let path = directory.path().join("slatedb");
        let shard = GraphShard::open_standalone_writer(path.to_string_lossy().as_ref(), Arc::new(InMemory::new()))
            .await
            .map_err(infrastructure)?;
        let session = HydraSession {
            shard,
            _directory: directory,
            scalar_parameters: BTreeMap::new(),
            list_parameters: BTreeMap::new(),
            request_sequence: 0,
        };
        Ok(session)
    }

    async fn fixture(&self, session: &mut Self::Session, fixture: &Kind) -> Result<(), AdapterFailure> {
        self.provision_fixture(session, fixture).await
    }

    async fn setup(&self, session: &mut Self::Session, query: &str) -> Result<(), AdapterFailure> {
        timeout(QUERY_TIMEOUT, self.execute_internal(session, query))
            .await
            .map_err(|_| AdapterFailure::Timeout)?
            .map(|_| ())
            .map_err(|error| graph_operation_failure("setup", None, error))
    }

    async fn parameters(&self, session: &mut Self::Session, values: &BTreeMap<String, Value>) -> Result<(), AdapterFailure> {
        session.scalar_parameters.clear();
        session.list_parameters.clear();
        for (name, value) in values {
            match value {
                Value::List(items) => {
                    let mut storage = Vec::with_capacity(items.len());
                    for item in items {
                        storage.push(to_property(item).map_err(AdapterFailure::Unsupported)?);
                    }
                    session.list_parameters.insert(name.clone(), storage);
                }
                Value::Map(_) | Value::Node(_) | Value::Relationship(_) | Value::Path(_) => {
                    return Err(AdapterFailure::Unsupported(format!(
                        "parameter ${name} uses a composite value unsupported by the production shard context"
                    )));
                }
                scalar => {
                    session
                        .scalar_parameters
                        .insert(name.clone(), to_property(scalar).map_err(AdapterFailure::Unsupported)?);
                }
            }
        }
        Ok(())
    }

    async fn syntax(&self, _session: &mut Self::Session, query: &str) -> Result<(), Diagnostic> {
        if let Some(depth) = excessive_collection_nesting(query) {
            return Err(Diagnostic {
                phase: "compile time".into(),
                error_type: "UnsupportedQuery".into(),
                detail: format!("collection literal nesting depth {depth} exceeds the safe parser boundary {MAX_SAFE_LITERAL_NESTING}"),
                stage: "parser-capability".into(),
                native_code: None,
                native_message: "production parser invocation suppressed before a known non-yielding input".into(),
            });
        }
        match self.mode {
            #[cfg(feature = "datafusion-cypher-engine")]
            CypherEngineMode::DataFusion => Err(datafusion_adapter_not_supported()),
            CypherEngineMode::Legacy => validate_opencypher_syntax(query).map_err(syntax_diagnostic),
            CypherEngineMode::Experimental => {
                if matches!(
                    prepare_opencypher_query_with_parameters(query, &BTreeMap::new(), &BTreeMap::new()),
                    Ok(PreparedOpenCypherQuery::Write(_))
                ) {
                    return validate_opencypher_syntax(query).map_err(syntax_diagnostic);
                }
                parse_cypher25_syntax(query).map(|_| ()).map_err(|error| Diagnostic {
                    phase: "compile time".into(),
                    error_type: "SyntaxError".into(),
                    detail: format!("Cypher 25 parser: {error:?}"),
                    stage: "parser".into(),
                    native_code: None,
                    native_message: format!("{error:?}"),
                })
            }
        }
    }

    async fn prepare(&self, session: &mut Self::Session, query: &str) -> Result<(), Diagnostic> {
        let legacy = prepare_opencypher_query_with_parameters(query, &session.scalar_parameters, &session.list_parameters);
        match self.mode {
            #[cfg(feature = "datafusion-cypher-engine")]
            CypherEngineMode::DataFusion => Err(datafusion_adapter_not_supported()),
            CypherEngineMode::Legacy => legacy.map(|_| ()).map_err(preparation_diagnostic),
            CypherEngineMode::Experimental => match legacy {
                Ok(PreparedOpenCypherQuery::Write(_)) => Ok(()),
                Ok(PreparedOpenCypherQuery::Read(_)) | Err(_) => {
                    validate_experimental_cypher_preparation(query, &session.scalar_parameters, &session.list_parameters)
                        .map_err(preparation_diagnostic)
                }
            },
        }
    }

    async fn execute(&self, session: &mut Self::Session, query: &str, source_line: usize) -> Result<QueryObservation, AdapterFailure> {
        let execution_path = self.execution_path(query, session);
        let outcome = match timeout(QUERY_TIMEOUT, self.execute_internal(session, query))
            .await
            .map_err(|_| AdapterFailure::Timeout)?
        {
            Ok(output) => rows_to_outcome(session, output, query)
                .await
                .map_err(|error| AdapterFailure::Unsupported(format!("{execution_path}: {error}")))?,
            Err(error) => QueryOutcome::Error(diagnostic(error)),
        };
        Ok(QueryObservation {
            engine: self.mode.as_str().into(),
            execution_path: execution_path.into(),
            source_line,
            parser: "accepted".into(),
            semantics: "prepared".into(),
            outcome,
        })
    }

    async fn observe_graph(&self, session: &mut Self::Session) -> Result<GraphState, AdapterFailure> {
        session.shard.refresh_storage_sequence(CELL_ID).await.map_err(infrastructure)?;
        let snapshot = session.shard.snapshot(CELL_ID).await.map_err(infrastructure)?;
        let inspection = snapshot.inspect_graph().await.map_err(infrastructure)?;
        Ok(graph_state(&inspection))
    }

    async fn close(&self, session: Self::Session) -> Result<(), AdapterFailure> {
        session.shard.close().await.map_err(infrastructure)
    }
}

fn excessive_collection_nesting(query: &str) -> Option<usize> {
    let mut quote = None;
    let mut escaped = false;
    let mut depth = 0usize;
    let mut maximum = 0usize;
    for character in query.chars() {
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
            continue;
        }
        match character {
            '\'' | '"' | '`' => quote = Some(character),
            '[' | '{' => {
                depth += 1;
                maximum = maximum.max(depth);
            }
            ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    (maximum > MAX_SAFE_LITERAL_NESTING).then_some(maximum)
}

#[test]
fn collection_nesting_guard_ignores_quoted_delimiters() {
    assert_eq!(excessive_collection_nesting("RETURN '[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[['"), None);
    assert_eq!(
        excessive_collection_nesting("RETURN [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]"),
        Some(34)
    );
}

async fn rows_to_outcome(session: &HydraSession, output: QueryOutput, query: &str) -> Result<QueryOutcome, String> {
    let (result, explicit_hydration) = match output {
        QueryOutput::Rows(result) => (result, None),
        QueryOutput::Mutation(mutation) => (mutation.into_result_set(), None),
        QueryOutput::Write(_) => (QueryResultSet::new(Vec::new(), Vec::new()), Some(Vec::new())),
        QueryOutput::Count(value) => (
            QueryResultSet::new(vec![QueryColumn::new("count")], vec![QueryRow::new(vec![QueryValue::Count(value)])]),
            Some(vec![Hydration::Scalar]),
        ),
        QueryOutput::Bool(value) => (
            QueryResultSet::new(vec![QueryColumn::new("value")], vec![QueryRow::new(vec![QueryValue::Bool(value)])]),
            Some(vec![Hydration::Scalar]),
        ),
        QueryOutput::Vertices(values) => (
            QueryResultSet::new(
                vec![QueryColumn::new("vertex")],
                values
                    .into_iter()
                    .map(|value| QueryRow::new(vec![QueryValue::VertexId(value)]))
                    .collect(),
            ),
            Some(vec![Hydration::Node]),
        ),
    };
    session
        .shard
        .refresh_storage_sequence(CELL_ID)
        .await
        .map_err(|error| error.to_string())?;
    let inspection = session
        .shard
        .snapshot(CELL_ID)
        .await
        .map_err(|error| error.to_string())?
        .inspect_graph()
        .await
        .map_err(|error| error.to_string())?;
    let hydration = explicit_hydration.unwrap_or_else(|| projection_hydration(query, &result.columns));
    let rows = result
        .rows
        .into_iter()
        .map(|row| {
            row.values
                .into_iter()
                .enumerate()
                .map(|(index, value)| query_value(value, &inspection, hydration.get(index).unwrap_or(&Hydration::Unknown)))
                .collect()
        })
        .collect::<Result<Vec<Vec<Value>>, String>>()?;
    Ok(QueryOutcome::Rows {
        columns: result.columns.into_iter().map(|column| column.name).collect(),
        rows,
    })
}

fn query_value(value: QueryValue, graph: &GraphInspection, hydration: &Hydration) -> Result<Value, String> {
    match value {
        QueryValue::Null => Ok(Value::Null),
        QueryValue::VertexId(id) => match hydration {
            Hydration::Node => node_value(id, graph),
            Hydration::Unknown => Err(format!("result vertex {id} has ambiguous scalar-or-node provenance")),
            _ => Ok(Value::Integer(i128::from(id))),
        },
        QueryValue::Count(value) => Ok(Value::Integer(i128::from(value))),
        QueryValue::Bool(value) => Ok(Value::Boolean(value)),
        QueryValue::Float(value) => Ok(Value::Float(value.0.to_bits())),
        QueryValue::Property(value) => property_value(&value),
        QueryValue::List(values) => values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                let child = match hydration {
                    Hydration::List(items) => items.get(index).unwrap_or(&Hydration::Unknown),
                    Hydration::ListEach(item) => item,
                    Hydration::Unknown => &Hydration::Unknown,
                    _ => &Hydration::Scalar,
                };
                query_value(value, graph, child)
            })
            .collect::<Result<_, _>>()
            .map(Value::List),
        QueryValue::Record(fields) => fields
            .into_iter()
            .map(|(key, value)| {
                let child = match hydration {
                    Hydration::Map(fields) => fields.get(&key).unwrap_or(&Hydration::Unknown),
                    Hydration::Unknown => &Hydration::Unknown,
                    _ => &Hydration::Scalar,
                };
                Ok((key, query_value(value, graph, child)?))
            })
            .collect::<Result<_, String>>()
            .map(Value::Map),
        QueryValue::Path(path) => path_value(*path),
        other => Err(format!("TCK value conversion does not support {other:?}")),
    }
}

fn node_value(id: u64, graph: &GraphInspection) -> Result<Value, String> {
    let metadata = graph
        .vertices
        .get(&id)
        .ok_or_else(|| format!("result vertex {id} is absent from the committed graph"))?;
    Ok(Value::Node(Node {
        labels: metadata.labels.iter().cloned().collect(),
        properties: metadata
            .properties
            .iter()
            .map(|(key, value)| Ok((key.clone(), property_value(value)?)))
            .collect::<Result<_, String>>()?,
    }))
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Hydration {
    Scalar,
    Node,
    List(Vec<Hydration>),
    ListEach(Box<Hydration>),
    Map(BTreeMap<String, Hydration>),
    Unknown,
}

/// Recover graph-value provenance from the typed production AST.
///
/// `QueryValue::VertexId` is the public numeric identity representation, but
/// whole-node projections use the same runtime variant. The adapter therefore
/// follows proven node bindings through projections and nested containers. An
/// ambiguous vertex value becomes an explicit capability limitation instead
/// of being fabricated as either an integer or a node.
fn projection_hydration(query: &str, columns: &[QueryColumn]) -> Vec<Hydration> {
    let Ok(document) = hydradb_cypher_parser_antlr::parse_cypher25(query) else {
        return vec![Hydration::Unknown; columns.len()];
    };
    let [Statement::Query(query)] = document.statements.as_slice() else {
        return vec![Hydration::Unknown; columns.len()];
    };
    if !query.unions.is_empty() || query.clauses.iter().any(|clause| matches!(clause, Clause::Next)) {
        return vec![Hydration::Unknown; columns.len()];
    }
    let mut bindings = BTreeMap::new();
    for clause in &query.clauses {
        match clause {
            Clause::Match(matched) => {
                bind_pattern_nodes(&matched.patterns, &mut bindings);
            }
            Clause::Create(created) => bind_pattern_nodes(&created.patterns, &mut bindings),
            Clause::Merge(merged) => bind_pattern_nodes(std::slice::from_ref(&merged.pattern), &mut bindings),
            Clause::With(projected) => bindings = projected_bindings(&projected.items, &bindings),
            Clause::Unwind(unwind) => {
                let item = list_item_hydration(&expression_hydration(&unwind.expression, &bindings));
                bindings.insert(unwind.variable.as_str().to_owned(), item);
            }
            Clause::Let(defined) => {
                for (name, expression) in &defined.bindings {
                    bindings.insert(name.as_str().to_owned(), expression_hydration(expression, &bindings));
                }
            }
            Clause::Return(returned) => return returned_hydration(&returned.items, &bindings, columns),
            Clause::Call(_) | Clause::OptionalBlock(_) | Clause::Syntax(_) | Clause::Next => {
                bindings.values_mut().for_each(|value| *value = Hydration::Unknown);
            }
            Clause::Filter(_)
            | Clause::Set(_)
            | Clause::Remove(_)
            | Clause::Delete(_)
            | Clause::Foreach(_)
            | Clause::OrderByAndPage(_)
            | Clause::Finish => {}
        }
    }
    vec![Hydration::Scalar; columns.len()]
}

fn bind_pattern_nodes(patterns: &[PathPattern], bindings: &mut BTreeMap<String, Hydration>) {
    for element in patterns.iter().flat_map(PathPattern::all_elements) {
        if let PatternElement::Node(node) = element {
            if let Some(variable) = &node.variable {
                bindings.insert(variable.as_str().to_owned(), Hydration::Node);
            }
        }
    }
}

fn projected_bindings(items: &ProjectionItems, bindings: &BTreeMap<String, Hydration>) -> BTreeMap<String, Hydration> {
    let mut projected = if items.includes_all() { bindings.clone() } else { BTreeMap::new() };
    for item in items.items() {
        if let Some(name) = item.output_binding() {
            projected.insert(name.as_str().to_owned(), expression_hydration(&item.expression, bindings));
        }
    }
    projected
}

fn returned_hydration(items: &ProjectionItems, bindings: &BTreeMap<String, Hydration>, columns: &[QueryColumn]) -> Vec<Hydration> {
    if !items.includes_all() {
        return items
            .items()
            .iter()
            .map(|item| expression_hydration(&item.expression, bindings))
            .collect();
    }
    let mut by_name = bindings.clone();
    for item in items.items() {
        if let Some(name) = item.output_binding() {
            by_name.insert(name.as_str().to_owned(), expression_hydration(&item.expression, bindings));
        }
    }
    columns
        .iter()
        .map(|column| by_name.get(&column.name).cloned().unwrap_or(Hydration::Unknown))
        .collect()
}

fn expression_hydration(expression: &Expression, bindings: &BTreeMap<String, Hydration>) -> Hydration {
    match expression {
        Expression::Identifier(name) => bindings.get(name.as_str()).cloned().unwrap_or(Hydration::Unknown),
        Expression::List(values) => Hydration::List(values.iter().map(|value| expression_hydration(value, bindings)).collect()),
        Expression::Map(fields) => Hydration::Map(
            fields
                .iter()
                .map(|(key, value)| (key.as_str().to_owned(), expression_hydration(value, bindings)))
                .collect(),
        ),
        Expression::Function { name, arguments, .. } => {
            let name = name.last().map(|name| name.as_str().to_ascii_lowercase()).unwrap_or_default();
            match (name.as_str(), arguments.as_slice()) {
                ("collect", [value]) => Hydration::ListEach(Box::new(expression_hydration(value, bindings))),
                ("head" | "last", [value]) => list_item_hydration(&expression_hydration(value, bindings)),
                ("min" | "max", [value]) => expression_hydration(value, bindings),
                ("coalesce", values) => common_hydration(values.iter().map(|value| expression_hydration(value, bindings))),
                ("id" | "count" | "labels" | "properties" | "type", _) => Hydration::Scalar,
                _ => Hydration::Unknown,
            }
        }
        Expression::Case { branches, default, .. } => common_hydration(
            branches
                .iter()
                .map(|(_, value)| expression_hydration(value, bindings))
                .chain(default.iter().map(|value| expression_hydration(value, bindings))),
        ),
        Expression::Let { bindings: local, body } => {
            let mut nested = bindings.clone();
            for (name, value) in local {
                nested.insert(name.as_str().to_owned(), expression_hydration(value, &nested));
            }
            expression_hydration(body, &nested)
        }
        // `list[index]` is one item of the list. A literal index into a list
        // whose items are known picks that item, so `[n, 0][0]` is the node;
        // otherwise any item could be selected, as with `head` and `last`.
        Expression::Index { target, index } => {
            let list = expression_hydration(target, bindings);
            match (&list, literal_index(index)) {
                (Hydration::List(items), Some(index)) => {
                    let position = if index < 0 { items.len() as i128 + index } else { index };
                    usize::try_from(position)
                        .ok()
                        .and_then(|position| items.get(position))
                        .cloned()
                        // Out of range is null, a scalar.
                        .unwrap_or(Hydration::Scalar)
                }
                _ => list_item_hydration(&list),
            }
        }
        Expression::Property { target, key } => match expression_hydration(target, bindings) {
            Hydration::Node => Hydration::Scalar,
            Hydration::Map(fields) => fields.get(key.as_str()).cloned().unwrap_or(Hydration::Unknown),
            Hydration::Scalar => Hydration::Scalar,
            _ => Hydration::Unknown,
        },
        Expression::Binary {
            left,
            operator: BinaryOperator::Add | BinaryOperator::Concatenate,
            right,
        } => concatenated_hydration(expression_hydration(left, bindings), expression_hydration(right, bindings)),
        Expression::Value(_) | Expression::Syntax(_) => Hydration::Unknown,
        Expression::Parameter(_)
        | Expression::Literal(_)
        | Expression::Unary { .. }
        | Expression::Binary { .. }
        | Expression::Exists(_)
        | Expression::Normalized { .. }
        | Expression::SessionUser
        | Expression::ValueKeyword { .. }
        | Expression::PathConstructor(_)
        | Expression::TemporalLiteral { .. }
        | Expression::TypeTest { .. }
        | Expression::Cast { .. }
        | Expression::ElementTest { .. } => Hydration::Scalar,
    }
}

/// The integer an index expression is written as: `0`, or `-1`, which parses
/// as unary minus applied to `1`.
fn literal_index(index: &Expression) -> Option<i128> {
    match index {
        Expression::Literal(Literal::Integer(value)) => Some(*value),
        Expression::Unary {
            operator: UnaryOperator::Minus,
            expression,
        } => match expression.as_ref() {
            Expression::Literal(Literal::Integer(value)) => Some(-*value),
            _ => None,
        },
        _ => None,
    }
}

fn list_item_hydration(value: &Hydration) -> Hydration {
    match value {
        Hydration::List(values) => common_hydration(values.iter().cloned()),
        Hydration::ListEach(value) => value.as_ref().clone(),
        Hydration::Unknown => Hydration::Unknown,
        _ => Hydration::Scalar,
    }
}

fn common_hydration(values: impl IntoIterator<Item = Hydration>) -> Hydration {
    let mut values = values.into_iter();
    let Some(first) = values.next() else {
        return Hydration::Scalar;
    };
    if values.all(|value| value == first) {
        first
    } else {
        Hydration::Unknown
    }
}

fn concatenated_hydration(left: Hydration, right: Hydration) -> Hydration {
    match (left, right) {
        (Hydration::List(mut left), Hydration::List(right)) => {
            left.extend(right);
            Hydration::List(left)
        }
        (Hydration::ListEach(left), Hydration::ListEach(right)) if left == right => Hydration::ListEach(left),
        (Hydration::Scalar, Hydration::Scalar) => Hydration::Scalar,
        (Hydration::Unknown, _) | (_, Hydration::Unknown) => Hydration::Unknown,
        _ => Hydration::Unknown,
    }
}

#[test]
fn projection_hydration_tracks_nodes_scalars_forwarding_and_containers() {
    let columns = |names: &[&str]| names.iter().map(|name| QueryColumn::new(*name)).collect::<Vec<_>>();
    assert_eq!(projection_hydration("MATCH (n) RETURN n", &columns(&["n"])), [Hydration::Node]);
    assert_eq!(
        projection_hydration("MATCH (n) RETURN n.id", &columns(&["n.id"])),
        [Hydration::Scalar]
    );
    assert_eq!(
        projection_hydration("MATCH (n) WITH n RETURN n", &columns(&["n"])),
        [Hydration::Node]
    );
    assert_eq!(
        projection_hydration("MATCH (n) RETURN [n, n.id] AS values", &columns(&["values"])),
        [Hydration::List(vec![Hydration::Node, Hydration::Scalar])]
    );
    assert_eq!(
        projection_hydration("MATCH (n) RETURN collect(n) AS nodes", &columns(&["nodes"])),
        [Hydration::ListEach(Box::new(Hydration::Node))]
    );
    assert_eq!(
        projection_hydration("MATCH (n) RETURN min(n.id) AS id", &columns(&["id"])),
        [Hydration::Scalar]
    );
    assert_eq!(projection_hydration("MATCH (n) RETURN *", &columns(&["n"])), [Hydration::Node]);
    assert_eq!(projection_hydration("CREATE (n) RETURN n", &columns(&["n"])), [Hydration::Node]);
    assert_eq!(projection_hydration("MERGE (n) RETURN n", &columns(&["n"])), [Hydration::Node]);
    assert_eq!(
        projection_hydration("MATCH (n) WITH {node: n} AS m RETURN m.node AS node", &columns(&["node"])),
        [Hydration::Node]
    );
    assert_eq!(
        projection_hydration("MATCH (n) RETURN [n] + [n.id] AS values", &columns(&["values"])),
        [Hydration::List(vec![Hydration::Node, Hydration::Scalar])]
    );
    assert_eq!(
        projection_hydration("MATCH (n) RETURN n AS x NEXT RETURN x.id AS id", &columns(&["id"])),
        [Hydration::Unknown]
    );
    assert_eq!(
        projection_hydration("MATCH (n) LET x = n RETURN x", &columns(&["x"])),
        [Hydration::Node]
    );
    assert_eq!(
        projection_hydration("MATCH (n) UNWIND [n] AS x RETURN x", &columns(&["x"])),
        [Hydration::Node]
    );
    assert_eq!(
        projection_hydration("MATCH (n) UNWIND [n, n.id] AS x RETURN x", &columns(&["x"])),
        [Hydration::Unknown]
    );
    // A literal index into a mixed list selects that item's provenance.
    assert_eq!(
        projection_hydration(
            "MATCH (n) RETURN [n, 0][0] AS a, [n, 0][1] AS b, [n, 0][-1] AS c, [n, 0][-2] AS d, [n, 0][5] AS e",
            &columns(&["a", "b", "c", "d", "e"])
        ),
        [
            Hydration::Node,
            Hydration::Scalar,
            Hydration::Scalar,
            Hydration::Node,
            Hydration::Scalar
        ]
    );
    // A non-literal index could select either item, so a mixed list stays
    // ambiguous; a uniform one does not.
    assert_eq!(
        projection_hydration(
            "MATCH (n) WITH n, 0 AS i RETURN [n, 0][i] AS mixed, [n, n][i] AS nodes",
            &columns(&["mixed", "nodes"])
        ),
        [Hydration::Unknown, Hydration::Node]
    );

    let graph = GraphInspection {
        vertices: BTreeMap::from([(1, hydradb::VertexMetadata::default().with_label("A"))]),
        relationships: BTreeMap::new(),
    };
    let hydration = concatenated_hydration(Hydration::List(vec![Hydration::Node]), Hydration::List(vec![Hydration::Scalar]));
    let value = query_value(
        QueryValue::List(vec![QueryValue::VertexId(1), QueryValue::VertexId(1)]),
        &graph,
        &hydration,
    )
    .expect("mixed graph list conversion");
    assert!(matches!(value, Value::List(values) if matches!(values.as_slice(), [Value::Node(_), Value::Integer(1)])));
    let error = query_value(QueryValue::VertexId(1), &graph, &Hydration::Unknown).expect_err("ambiguous identity must not be guessed");
    assert!(error.contains("ambiguous scalar-or-node provenance"));
}

fn path_value(path: QueryPath) -> Result<Value, String> {
    let mut nodes = path.nodes.into_iter();
    let start = nodes.next().ok_or_else(|| "query path has no start node".to_string())?;
    let mut previous_id = start.id;
    let start = Node {
        labels: start.labels.into_iter().collect(),
        properties: start
            .properties
            .iter()
            .map(|(key, value)| Ok((key.clone(), property_value(value)?)))
            .collect::<Result<_, String>>()?,
    };
    let mut segments = Vec::with_capacity(path.relationships.len());
    for (relationship, node) in path.relationships.into_iter().zip(nodes) {
        let direction = if relationship.src == relationship.dst && relationship.src == previous_id && node.id == previous_id {
            return Err("self-loop path traversal direction is unavailable in the production path payload".into());
        } else if relationship.src == previous_id && relationship.dst == node.id {
            Direction::Outgoing
        } else if relationship.dst == previous_id && relationship.src == node.id {
            Direction::Incoming
        } else {
            return Err("query path relationship does not connect adjacent nodes".into());
        };
        previous_id = node.id;
        segments.push((
            direction,
            Relationship {
                relationship_type: relationship.edge_type,
                properties: relationship
                    .properties
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), property_value(value)?)))
                    .collect::<Result<_, String>>()?,
            },
            Node {
                labels: node.labels.into_iter().collect(),
                properties: node
                    .properties
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), property_value(value)?)))
                    .collect::<Result<_, String>>()?,
            },
        ));
    }
    Ok(Value::Path(Path { start, segments }))
}

#[test]
fn self_loop_path_is_not_assigned_an_invented_direction() {
    let node = QueryPathNode {
        id: 1,
        labels: Vec::new(),
        properties: BTreeMap::new(),
    };
    let path = QueryPath {
        nodes: vec![node.clone(), node],
        relationships: vec![QueryPathRelationship {
            id: Some(1),
            edge_type: "LOOP".into(),
            src: 1,
            dst: 1,
            properties: BTreeMap::new(),
        }],
    };
    assert_eq!(
        path_value(path).unwrap_err(),
        "self-loop path traversal direction is unavailable in the production path payload"
    );
}

fn graph_state(graph: &GraphInspection) -> GraphState {
    let mut state = GraphState::default();
    for (id, metadata) in &graph.vertices {
        let identity = format!("v:{id}");
        state.nodes.insert(identity.clone());
        state
            .labels
            .extend(metadata.labels.iter().cloned().map(|label| (identity.clone(), label)));
        for (key, value) in &metadata.properties {
            state.properties.insert(
                (identity.clone(), key.clone()),
                property_value(value).expect("stored property converts"),
            );
        }
    }
    for (id, relationship) in &graph.relationships {
        let identity = format!("r:{id}");
        state.relationships.insert(identity.clone());
        for (key, value) in &relationship.metadata.properties {
            state.properties.insert(
                (identity.clone(), key.clone()),
                property_value(value).expect("stored property converts"),
            );
        }
    }
    state
}

fn property_value(value: &VertexPropertyValue) -> Result<Value, String> {
    match value {
        VertexPropertyValue::Integer(value) => Ok(Value::Integer(i128::from(*value))),
        VertexPropertyValue::SignedInteger(value) => Ok(Value::Integer(i128::from(*value))),
        VertexPropertyValue::Bool(value) => Ok(Value::Boolean(*value)),
        VertexPropertyValue::Float(value) => Ok(Value::Float(value.0.to_bits())),
        VertexPropertyValue::String(value) => Ok(Value::String(value.clone())),
        VertexPropertyValue::Date(value) => {
            use chrono::{Days, NaiveDate};

            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("Unix epoch is valid");
            let date = if *value >= 0 {
                epoch.checked_add_days(Days::new(u64::from(value.unsigned_abs())))
            } else {
                epoch.checked_sub_days(Days::new(u64::from(value.unsigned_abs())))
            }
            .ok_or_else(|| "stored date exceeds the conformance adapter range".to_string())?;
            Ok(Value::String(date.format("%Y-%m-%d").to_string()))
        }
        VertexPropertyValue::List(values) => values
            .clone()
            .into_property_values()
            .iter()
            .map(property_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::List),
    }
}

fn to_property(value: &Value) -> Result<VertexPropertyValue, String> {
    match value {
        Value::Null => Err("null parameters are not representable by the production shard context".into()),
        Value::Boolean(value) => Ok(VertexPropertyValue::Bool(*value)),
        Value::Integer(value) => match (i64::try_from(*value), u64::try_from(*value)) {
            (Ok(value), _) => Ok(VertexPropertyValue::from_i64(value)),
            (_, Ok(value)) => Ok(VertexPropertyValue::Integer(value)),
            _ => Err(format!("integer {value} exceeds the production parameter range")),
        },
        Value::Float(bits) => Ok(VertexPropertyValue::Float(QueryFloat(f64::from_bits(*bits)))),
        Value::String(value) => Ok(VertexPropertyValue::String(value.clone())),
        _ => Err("composite value is not a scalar property".into()),
    }
}

fn named_graph_script(directory: &std::path::Path, script: &str) -> std::path::PathBuf {
    let script = std::path::Path::new(script);
    if script.extension().is_some() {
        directory.join(script)
    } else {
        directory.join(script).with_extension("cypher")
    }
}

#[test]
fn named_graph_scripts_follow_the_pinned_extension_convention() {
    let directory = std::path::Path::new("graphs/binary-tree-1");
    assert_eq!(
        named_graph_script(directory, "binary-tree-1"),
        directory.join("binary-tree-1.cypher")
    );
    assert_eq!(named_graph_script(directory, "custom.cypher"), directory.join("custom.cypher"));
}

fn diagnostic(error: GraphError) -> Diagnostic {
    let error_type = match &error {
        GraphError::MissingQueryParameter { .. } => "ParameterMissing",
        GraphError::UnsupportedQuery { .. } | GraphError::UnclassifiedQuery { .. } => "UnsupportedQuery",
        _ => "ExecutionError",
    };
    Diagnostic {
        phase: "runtime".into(),
        error_type: error_type.to_string(),
        detail: error.to_string(),
        stage: "execution".into(),
        native_code: None,
        native_message: error.to_string(),
    }
}

fn syntax_diagnostic(error: GraphError) -> Diagnostic {
    let mut diagnostic = diagnostic(error);
    diagnostic.phase = "compile time".into();
    diagnostic.error_type = "SyntaxError".into();
    diagnostic.stage = "parser".into();
    diagnostic
}

fn preparation_diagnostic(error: GraphError) -> Diagnostic {
    let mut diagnostic = diagnostic(error);
    diagnostic.phase = "compile time".into();
    diagnostic.stage = "semantics".into();
    diagnostic
}

fn infrastructure(error: GraphError) -> AdapterFailure {
    AdapterFailure::Infrastructure(error.to_string())
}

fn graph_operation_failure(stage: &str, path: Option<&std::path::Path>, error: GraphError) -> AdapterFailure {
    let message = path.map_or_else(
        || format!("{stage}: {error}"),
        |path| {
            let identity = path.file_name().and_then(|name| name.to_str()).unwrap_or("unknown script");
            format!("{stage} script {identity}: {error}")
        },
    );
    match error {
        GraphError::UnsupportedQuery { .. } | GraphError::UnclassifiedQuery { .. } => AdapterFailure::Unsupported(message),
        _ if stage == "fixture" => AdapterFailure::Fixture(message),
        _ => AdapterFailure::Setup(message),
    }
}

// This adapter deliberately compares the legacy and experimental routes. Do not
// claim DataFusion conformance by silently running another engine's preparer.
#[cfg(feature = "datafusion-cypher-engine")]
fn datafusion_adapter_not_supported() -> Diagnostic {
    Diagnostic {
        phase: "compile time".into(),
        error_type: "UnsupportedQuery".into(),
        detail: "this TCK adapter supports legacy and experimental modes only".into(),
        stage: "adapter-capability".into(),
        native_code: None,
        native_message: "DataFusion requires its own conformance adapter".into(),
    }
}
