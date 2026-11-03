use hydradb_cypher_ast::QueryFailureReason;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use indexmap::{IndexMap, IndexSet};
use std::future::Future;
use std::pin::Pin;

use crate::{
    AggregateFunction, BoundValue, GraphPhysicalPlan, PatternDirection, PhysicalBinaryOperator,
    PhysicalExpression, PhysicalSort, PhysicalUnaryOperator, ScalarFunction, ScalarValue, Symbol,
};

use crate::capability::{
    shape_error, NESTED_PROJECT_UNNAMED, RESULT_PLAN_ROOT, UNION_COLUMN_MISMATCH, UNION_EMPTY,
};
use crate::hydration::{plan_reuses_hydration, HydrationIdentityMap};
use crate::model::{compare_property_values, scalar_value_rank};
use crate::prepare::{prepare_query, prepare_query_with_statistics};
use crate::{
    scalar_values_equal, AccessPathKind, EngineError, EngineResult, ExpandDirection,
    ExpandNeighbor, ExpandRequest, GraphRead, GraphStorage, GuardedAccess,
    OrderedPropertyScanRequest, OrderedRelationshipPropertyScanRequest, OrderedScanPosition,
    ParameterMap, PreparedQuery, PropertyBound, PropertyEqualityCandidate, QueryColumn,
    QueryResult, QueryRow, QueryValue, ReadRequest, RelationshipId, RelationshipRecord,
    SortDirection, StatisticsAccessReport, VertexId, VertexRecord,
};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
struct BindingRow {
    vertices: BTreeMap<Symbol, VertexRecord>,
    relationships: BTreeMap<Symbol, RelationshipRecord>,
    /// Names an OPTIONAL match tried to bind and could not.
    ///
    /// Cypher binds those to null rather than leaving them unbound, and the
    /// difference decides what a later clause does with them. A null name
    /// matches no pattern, so a required clause reading one produces nothing
    /// and the row is eliminated. An *absent* name is merely one this plan has
    /// not produced yet, and must be scanned for. Collapsing the two lets a
    /// later clause scan for a name Cypher says is null and bind something
    /// arbitrary to it.
    nulls: BTreeSet<Symbol>,
    /// Scalars a projection bound to a name, which no pattern can produce.
    ///
    /// `WITH e.entity_id AS id` binds a value, not an entity, and the rest of
    /// the query must be able to read `id` the same way it reads `e`. An entity
    /// projected under a new name stays in `vertices` or `relationships`
    /// instead, because flattening it to an id would lose the property map that
    /// `id.name` needs.
    values: BTreeMap<Symbol, QueryValue>,
    /// Full records referenced by a named vertex list, shared across aliases.
    vertex_lists: BTreeMap<Symbol, std::sync::Arc<Vec<VertexRecord>>>,
    /// Full records for a named relationship list, shared across aliases.
    relationship_lists: BTreeMap<Symbol, std::sync::Arc<Vec<RelationshipRecord>>>,
}

impl BindingRow {
    /// Whether `binding` is bound to null by an OPTIONAL that matched nothing.
    fn is_null(&self, binding: &Symbol) -> bool {
        self.nulls.contains(binding)
    }

    /// Record every name in `bindings` this row does not carry as null. Used
    /// on the miss side of an optional join, where the right plan's bindings
    /// are exactly the names that failed to bind.
    fn mark_null(&mut self, bindings: &BTreeSet<Symbol>) {
        for binding in bindings {
            if !self.vertices.contains_key(binding)
                && !self.relationships.contains_key(binding)
                && !self.values.contains_key(binding)
            {
                self.nulls.insert(binding.clone());
            }
        }
    }
}

struct ExpandSpec<'a> {
    from: &'a Symbol,
    relationship: Option<&'a Symbol>,
    relationship_types: &'a [Symbol],
    direction: PatternDirection,
    to: &'a Symbol,
    target_labels: &'a [Symbol],
}

#[derive(Clone, Default)]
struct TraversalHistory {
    used: std::sync::Arc<BTreeSet<RelationshipId>>,
    /// Unnamed walks retain only the trail membership set.
    path: Option<std::sync::Arc<Vec<RelationshipRecord>>>,
}

impl TraversalHistory {
    fn records(&self) -> &[RelationshipRecord] {
        self.path.as_deref().map(Vec::as_slice).unwrap_or_default()
    }

    fn extend(mut self, relationship: &RelationshipRecord) -> Self {
        std::sync::Arc::make_mut(&mut self.used).insert(relationship.id);
        if let Some(path) = &mut self.path {
            std::sync::Arc::make_mut(path).push(relationship.clone());
        }
        self
    }
}

#[derive(Clone)]
struct TraversalState {
    row: BindingRow,
    current: VertexId,
    history: TraversalHistory,
}

#[derive(Clone, Debug)]
enum EvaluatedValue {
    Null,
    Vertex(VertexId),
    Relationship(RelationshipId),
    Scalar(ScalarValue),
    Truth(Option<bool>),
    /// A list value: a literal, a list parameter, a collected aggregate read
    /// back from a projection, or an UNWIND source.
    List(Vec<EvaluatedValue>),
}

pub struct CypherEngine<S> {
    storage: S,
}

impl<S> CypherEngine<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    pub fn prepare(&self, query: &str, parameters: &ParameterMap) -> EngineResult<PreparedQuery> {
        prepare_query(query, parameters)
    }

    /// Preparation with list-valued parameters, which `IN` needs. `prepare`
    /// stays list-free so existing callers are unaffected.
    pub fn prepare_with_lists(
        &self,
        query: &str,
        parameters: &ParameterMap,
        lists: &crate::ListParameterMap,
    ) -> EngineResult<PreparedQuery> {
        prepare_query_with_statistics(query, parameters, lists, &crate::UnknownStatistics)
    }

    /// Lower without selecting a physical plan. This preserves both parameter
    /// maps while a storage adapter loads statistics for the pinned snapshot.
    pub fn lower_with_lists(
        &self,
        query: &str,
        parameters: &ParameterMap,
        lists: &crate::ListParameterMap,
    ) -> EngineResult<crate::LogicalQuery> {
        crate::prepare::lower_query(query, parameters, lists)
    }

    pub fn prepare_with_statistics(
        &self,
        query: &str,
        parameters: &ParameterMap,
        statistics: &dyn crate::StatisticsProvider,
    ) -> EngineResult<PreparedQuery> {
        prepare_query_with_statistics(
            query,
            parameters,
            &crate::ListParameterMap::new(),
            statistics,
        )
    }
}

impl<S: GraphStorage> CypherEngine<S> {
    pub async fn execute(
        &self,
        query: &str,
        parameters: &ParameterMap,
    ) -> EngineResult<QueryResult> {
        let prepared = self.prepare(query, parameters)?;
        self.execute_prepared(&prepared).await
    }

    /// Executes the exact physical artifact held by `prepared` without
    /// re-parsing, re-lowering, or re-planning.
    ///
    /// Capability is settled before the read session opens. It used to be
    /// settled by whichever operator first refused the plan, which meant an
    /// unsupported shape had already cost a `begin_read` - and, through a
    /// storage adapter, a snapshot - before anyone could say it was
    /// unsupported.
    pub async fn execute_prepared(&self, prepared: &PreparedQuery) -> EngineResult<QueryResult> {
        prepared.capability().into_result()?;
        let mut read = self.storage.begin_read(ReadRequest::default()).await?;
        // Only a plan that can reach the same vertex twice is wrapped: the map
        // is a cost on every other shape and a saving on none of them.
        if !plan_reuses_hydration(&prepared.physical) {
            return execute_result_plan(read.as_mut(), &prepared.physical).await;
        }
        let mut read = HydrationIdentityMap::new(read.as_mut());
        execute_result_plan(&mut read, &prepared.physical).await
    }
}

// Operator dispatch. Every physical operator invocation enters through one of
// the two `execute_*_plan` wrappers below, which bracket the real dispatch with
// `GraphRead::operator_started`/`operator_finished`. That pair is the whole of
// the executor's profiling surface: operator bodies stay unaware of it, and a
// new operator is observed the moment it is dispatched.

fn execute_result_plan<'a>(
    read: &'a mut dyn GraphRead,
    plan: &'a GraphPhysicalPlan,
) -> Pin<Box<dyn Future<Output = EngineResult<QueryResult>> + Send + 'a>> {
    Box::pin(async move {
        read.operator_started(plan);
        let result = dispatch_result_plan(&mut *read, plan).await;
        read.operator_finished(plan, result.as_ref().map_or(0, |result| result.rows.len()));
        result
    })
}

fn dispatch_result_plan<'a>(
    read: &'a mut dyn GraphRead,
    plan: &'a GraphPhysicalPlan,
) -> Pin<Box<dyn Future<Output = EngineResult<QueryResult>> + Send + 'a>> {
    Box::pin(async move {
        let GraphPhysicalPlan::Union { arms, all } = plan else {
            return execute_project(read, plan).await;
        };
        let mut combined: Option<QueryResult> = None;
        // `UNION` keeps the first occurrence of every row across all arms,
        // including duplicates inside the first arm, as legacy does. An
        // `IndexSet` is that rule: insertion-ordered, and an insert of a row
        // already held keeps the one already held.
        let mut deduped: IndexSet<SemanticRow> = IndexSet::new();
        let mut row_ordinal = 0;
        for arm in arms {
            let result = execute_result_plan(read, arm).await?;
            let combined = match &mut combined {
                Some(combined) => {
                    if combined.columns != result.columns {
                        return Err(shape_error(
                            UNION_COLUMN_MISMATCH,
                            "all UNION arms must return the same column names",
                        ));
                    }
                    combined
                }
                None => combined.insert(QueryResult {
                    columns: result.columns,
                    rows: Vec::new(),
                }),
            };
            if *all {
                combined.rows.extend(result.rows);
                continue;
            }
            // Accumulated in the set rather than pushed through it, so the
            // rows are held once. `combined.rows` takes them below.
            for row in result.rows {
                read.checkpoint("cypher_union_rows")?;
                deduped.insert(SemanticRow::new(row, row_ordinal));
                row_ordinal += 1;
            }
        }
        if !*all {
            if let Some(combined) = &mut combined {
                combined.rows = deduped.into_iter().map(|row| row.0).collect();
            }
        }
        combined.ok_or_else(|| shape_error(UNION_EMPTY, "UNION requires at least one arm"))
    })
}

async fn execute_project(
    read: &mut dyn GraphRead,
    plan: &GraphPhysicalPlan,
) -> EngineResult<QueryResult> {
    let GraphPhysicalPlan::Project {
        input,
        items,
        distinct,
        post_sort,
        post_skip,
        post_limit,
    } = plan
    else {
        return Err(shape_error(
            RESULT_PLAN_ROOT,
            "a result-producing plan must have Project at its root",
        ));
    };
    let binding_rows = execute_binding_plan(read, input, None).await?;
    let columns = items
        .iter()
        .map(|item| QueryColumn {
            name: item.column_name(),
        })
        .collect::<Vec<_>>();
    let aggregate = items
        .iter()
        .any(|item| expression_contains_aggregate(&item.expression));
    let mut projected = if aggregate {
        aggregate_project_rows(read, &binding_rows, items, post_sort)?
    } else {
        binding_rows
            .iter()
            .map(|binding_row| {
                let values = items
                    .iter()
                    .map(|item| {
                        evaluate_expression(&item.expression, binding_row).map(to_query_value)
                    })
                    .collect::<EngineResult<Vec<_>>>()?;
                Ok((QueryRow { values }, Vec::new()))
            })
            .collect::<EngineResult<Vec<_>>>()?
    };
    if *distinct {
        // Keyed by the row, carrying its sort keys. `or_insert` keeps the
        // first occurrence's keys, and the map keeps first-seen order.
        let mut unique: IndexMap<SemanticRow, Vec<QueryValue>> =
            IndexMap::with_capacity(projected.len());
        for (ordinal, (row, keys)) in projected.into_iter().enumerate() {
            read.checkpoint("cypher_distinct_rows")?;
            unique.entry(SemanticRow::new(row, ordinal)).or_insert(keys);
        }
        projected = unique
            .into_iter()
            .map(|(row, keys)| (row.0, keys))
            .collect();
    }
    if aggregate && !post_sort.is_empty() {
        projected
            .sort_by(|(_, left), (_, right)| compare_projected_sort_keys(left, right, post_sort));
    }
    let skip = post_skip
        .as_ref()
        .map(|count| window_count(count, "SKIP"))
        .transpose()?
        .unwrap_or(0);
    let limit = post_limit
        .as_ref()
        .map(|count| window_count(count, "LIMIT"))
        .transpose()?;
    let mut rows = projected
        .into_iter()
        .skip(skip)
        .map(|(row, _)| row)
        .collect::<Vec<_>>();
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    Ok(QueryResult { columns, rows })
}

/// Project binding rows into a new scope.
///
/// The output rows carry only what the projection names. An item that is a
/// bare binding carries its record across, so a vertex projected under a new
/// name keeps its properties; anything else is evaluated to a value and bound
/// by its alias. Ordering, DISTINCT and the window apply *after* projection,
/// against the projected scope, which is where Cypher puts them.
fn project_binding_rows(
    read: &mut dyn GraphRead,
    input_rows: Vec<BindingRow>,
    items: &[crate::PhysicalProjection],
    distinct: bool,
    post_sort: &[PhysicalSort],
    post_skip: &Option<BoundValue>,
    post_limit: &Option<BoundValue>,
) -> EngineResult<Vec<BindingRow>> {
    let aggregate = items
        .iter()
        .any(|item| expression_contains_aggregate(&item.expression));
    let mut projected = if aggregate {
        Vec::new()
    } else {
        Vec::with_capacity(input_rows.len())
    };
    if aggregate {
        // Grouping at the boundary: one output row per group of the items
        // that do not aggregate, exactly as RETURN groups. A grouping key
        // that is a bare binding carries its entity across, so
        // `WITH n, count(*) AS c MATCH (n)-->()` still knows `n`.
        for group in group_binding_rows(read, &input_rows, items)?.into_values() {
            let mut output = BindingRow::default();
            for item in items {
                let name = projected_symbol(item)?;
                match (&item.expression, group.first()) {
                    (PhysicalExpression::Binding(source), Some(first)) => {
                        carry_binding(first, source, name, &mut output);
                    }
                    // A relationship's `.id` is a value, not the relationship.
                    (PhysicalExpression::Identity(source), Some(first))
                        if bound_relationship(first, source).is_none() =>
                    {
                        carry_binding(first, source, name, &mut output);
                    }
                    (expression, _) => {
                        let value = evaluate_group_expression(expression, &group)?;
                        bind_computed_value(group.iter().copied(), name, value, &mut output);
                    }
                }
            }
            projected.push(output);
        }
    }
    for row in input_rows.iter().filter(|_| !aggregate) {
        let mut output = BindingRow::default();
        for item in items {
            let name = projected_symbol(item)?;
            match &item.expression {
                PhysicalExpression::Binding(source) => {
                    carry_binding(row, source, name, &mut output);
                }
                // A relationship's `.id` is a value, not the relationship.
                PhysicalExpression::Identity(source)
                    if bound_relationship(row, source).is_none() =>
                {
                    carry_binding(row, source, name, &mut output);
                }
                expression => {
                    let value = to_query_value(evaluate_expression(expression, row)?);
                    bind_computed_value(std::iter::once(row), name, value, &mut output);
                }
            }
        }
        projected.push(output);
    }

    if distinct {
        // Same change as the binding-level UNION above, for the same reason.
        let mut unique: IndexSet<SemanticBindingRow> = IndexSet::with_capacity(projected.len());
        for (ordinal, row) in projected.into_iter().enumerate() {
            read.checkpoint("cypher_project_distinct")?;
            unique.insert(SemanticBindingRow::new(row, ordinal));
        }
        projected = unique.into_iter().map(|row| row.0).collect();
    }

    if !post_sort.is_empty() {
        let mut keyed = projected
            .into_iter()
            .map(|row| {
                let keys = post_sort
                    .iter()
                    .map(|item| evaluate_expression(&item.expression, &row).map(to_query_value))
                    .collect::<EngineResult<Vec<_>>>()?;
                Ok((row, keys))
            })
            .collect::<EngineResult<Vec<_>>>()?;
        keyed.sort_by(|(_, left), (_, right)| compare_projected_sort_keys(left, right, post_sort));
        projected = keyed.into_iter().map(|(row, _)| row).collect();
    }

    let skip = post_skip
        .as_ref()
        .map(|count| window_count(count, "SKIP"))
        .transpose()?
        .unwrap_or(0);
    let limit = post_limit
        .as_ref()
        .map(|count| window_count(count, "LIMIT"))
        .transpose()?;
    let mut rows = projected.into_iter().skip(skip).collect::<Vec<_>>();
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

/// Carry `source` from `row` into `output` under `name`.
///
/// The entity, not its id: `WITH n AS m` must leave `m.name` readable, and an
/// id cannot answer that.
fn carry_binding(row: &BindingRow, source: &Symbol, name: Symbol, output: &mut BindingRow) {
    if let Some(vertex) = row.vertices.get(source) {
        output.vertices.insert(name, vertex.clone());
    } else if let Some(relationship) = row.relationships.get(source) {
        output.relationships.insert(name, relationship.clone());
    } else if row.is_null(source) {
        // A name an OPTIONAL left null stays *null-bound*, not merely valued
        // null. `nulls` documents why the two cannot be collapsed: a later
        // clause scanning for a name Cypher says is null would bind something
        // arbitrary to it. Projecting it into `values` alone did exactly that
        // -- `OPTIONAL MATCH (a)-[:R]->(p) WITH p MATCH (p:Person)` scanned
        // Person and answered a cross product where the row should have gone.
        output.nulls.insert(name);
    } else if let Some(value) = row.values.get(source) {
        bind_computed_value(std::iter::once(row), name, value.clone(), output);
    } else {
        // Not in scope at all rather than null-bound. Lowering rejects that,
        // so this is a belt-and-braces null and is marked as one for the same
        // reason as above.
        output.nulls.insert(name);
    }
}

/// Bind computed entities with the records needed by later property reads.
fn bind_computed_value<'a>(
    rows: impl Iterator<Item = &'a BindingRow> + Clone,
    name: Symbol,
    value: QueryValue,
    output: &mut BindingRow,
) {
    if let Some(records) = retain_vertex_records(rows.clone(), &value) {
        if let QueryValue::VertexId(id) = &value {
            if let Some(record) = records.iter().find(|record| record.id == *id) {
                output.vertices.insert(name, record.clone());
                return;
            }
        }
        output.vertex_lists.insert(name.clone(), records);
    }
    if let Some(records) = retain_relationship_records(rows, &value) {
        output.relationship_lists.insert(name.clone(), records);
    }
    output.values.insert(name, value);
}

/// Carry only vertex records referenced by a computed result.
fn retain_vertex_records<'a>(
    rows: impl IntoIterator<Item = &'a BindingRow>,
    value: &QueryValue,
) -> Option<std::sync::Arc<Vec<VertexRecord>>> {
    fn identities(value: &QueryValue, into: &mut BTreeSet<VertexId>) {
        match value {
            QueryValue::VertexId(id) => {
                into.insert(*id);
            }
            QueryValue::List(values) => {
                for value in values {
                    identities(value, into);
                }
            }
            _ => {}
        }
    }

    let mut wanted = BTreeSet::new();
    identities(value, &mut wanted);
    if wanted.is_empty() {
        return None;
    }

    let mut found = BTreeMap::new();
    for row in rows {
        for records in row.vertex_lists.values() {
            if records.len() == wanted.len()
                && records.iter().all(|record| wanted.contains(&record.id))
            {
                return Some(records.clone());
            }
        }
        for record in row
            .vertices
            .values()
            .chain(row.vertex_lists.values().flat_map(|records| records.iter()))
        {
            if wanted.contains(&record.id) {
                found.entry(record.id).or_insert_with(|| record.clone());
            }
        }
    }
    (!found.is_empty()).then(|| std::sync::Arc::new(found.into_values().collect()))
}

/// Carry only records referenced by a computed result, including nested lists
/// and collected relationships. This metadata is not part of logical row keys.
fn retain_relationship_records<'a>(
    rows: impl IntoIterator<Item = &'a BindingRow>,
    value: &QueryValue,
) -> Option<std::sync::Arc<Vec<RelationshipRecord>>> {
    fn identities(value: &QueryValue, into: &mut BTreeSet<RelationshipId>) {
        match value {
            QueryValue::RelationshipId(id) => {
                into.insert(*id);
            }
            QueryValue::List(values) => {
                for value in values {
                    identities(value, into);
                }
            }
            _ => {}
        }
    }
    let mut wanted = BTreeSet::new();
    identities(value, &mut wanted);
    if wanted.is_empty() {
        return None;
    }
    let mut found = BTreeMap::new();
    for row in rows {
        for records in row.relationship_lists.values() {
            if records.len() == wanted.len()
                && records.iter().all(|record| wanted.contains(&record.id))
            {
                return Some(records.clone());
            }
        }
        for record in row.relationships.values().chain(
            row.relationship_lists
                .values()
                .flat_map(|records| records.iter()),
        ) {
            if wanted.contains(&record.id) {
                found.entry(record.id).or_insert_with(|| record.clone());
            }
        }
    }
    (!found.is_empty()).then(|| std::sync::Arc::new(found.into_values().collect()))
}

/// Group `rows` by the values of the items that do not aggregate.
///
/// Groups keep first-seen order, and the key is structural value equality
/// (see `QueryValue`), so every NULL key forms one group. With no grouping
/// keys, no input still forms one empty group: `count(*)` over nothing is a
/// row saying 0, not no row.
fn group_binding_rows<'a>(
    read: &dyn GraphRead,
    rows: &'a [BindingRow],
    items: &[crate::PhysicalProjection],
) -> EngineResult<IndexMap<SemanticValues, Vec<&'a BindingRow>>> {
    let mut grouping = Vec::new();
    for item in items {
        collect_grouping_expressions(&item.expression, &mut grouping);
    }
    let mut groups: IndexMap<SemanticValues, Vec<&BindingRow>> = IndexMap::new();
    for (ordinal, row) in rows.iter().enumerate() {
        read.checkpoint("cypher_group_rows")?;
        let key = grouping
            .iter()
            .map(|expression| evaluate_expression(expression, row).map(to_query_value))
            .collect::<EngineResult<Vec<_>>>()?;
        groups
            .entry(SemanticValues::new(key, ordinal))
            .or_default()
            .push(row);
    }
    if groups.is_empty() && grouping.is_empty() {
        groups.insert(SemanticValues::new(Vec::new(), 0), Vec::new());
    }
    Ok(groups)
}

/// The name a projected item binds in the new scope.
///
/// An alias names itself. A bare binding keeps its own name, which is what
/// makes `WITH a, b` a pass-through. Anything else has no name to be read by
/// later, and is refused rather than invented - the same answer the capability
/// check gives before execution starts.
fn projected_symbol(item: &crate::PhysicalProjection) -> EngineResult<Symbol> {
    if let Some(alias) = &item.alias {
        return Ok(alias.clone());
    }
    match &item.expression {
        PhysicalExpression::Binding(binding) | PhysicalExpression::Identity(binding) => {
            Ok(binding.clone())
        }
        _ => Err(shape_error(
            NESTED_PROJECT_UNNAMED,
            "a projection below a result plan must alias every computed item",
        )),
    }
}

pub(crate) fn expression_contains_aggregate(expression: &PhysicalExpression) -> bool {
    match expression {
        PhysicalExpression::Aggregate { .. } => true,
        PhysicalExpression::InList { expression, .. } => expression_contains_aggregate(expression),
        PhysicalExpression::Unary { expression, .. } => expression_contains_aggregate(expression),
        PhysicalExpression::Function { arguments, .. } | PhysicalExpression::List(arguments) => {
            arguments.iter().any(expression_contains_aggregate)
        }
        PhysicalExpression::Case { .. } => case_parts(expression)
            .into_iter()
            .any(expression_contains_aggregate),
        PhysicalExpression::Binary { left, right, .. } => {
            expression_contains_aggregate(left) || expression_contains_aggregate(right)
        }
        PhysicalExpression::Binding(_)
        | PhysicalExpression::Identity(_)
        | PhysicalExpression::Property { .. }
        | PhysicalExpression::Value(_) => false,
    }
}

fn aggregate_project_rows(
    read: &dyn GraphRead,
    rows: &[BindingRow],
    items: &[crate::PhysicalProjection],
    sort: &[PhysicalSort],
) -> EngineResult<Vec<(QueryRow, Vec<QueryValue>)>> {
    group_binding_rows(read, rows, items)?
        .into_iter()
        .map(|(_, group)| {
            let values = items
                .iter()
                .map(|item| evaluate_group_expression(&item.expression, &group))
                .collect::<EngineResult<Vec<_>>>()?;
            let sort_values = sort
                .iter()
                .map(|item| evaluate_group_expression(&item.expression, &group))
                .collect::<EngineResult<Vec<_>>>()?;
            Ok((QueryRow { values }, sort_values))
        })
        .collect()
}

fn collect_grouping_expressions<'a>(
    expression: &'a PhysicalExpression,
    grouping: &mut Vec<&'a PhysicalExpression>,
) {
    if matches!(expression, PhysicalExpression::Aggregate { .. }) {
        return;
    }
    if !expression_contains_aggregate(expression) {
        if expression_requires_row(expression) && !grouping.contains(&expression) {
            grouping.push(expression);
        }
        return;
    }
    match expression {
        PhysicalExpression::Unary { expression, .. } => {
            collect_grouping_expressions(expression, grouping);
        }
        PhysicalExpression::Function { arguments, .. } | PhysicalExpression::List(arguments) => {
            for argument in arguments {
                collect_grouping_expressions(argument, grouping);
            }
        }
        PhysicalExpression::Case { .. } => {
            for part in case_parts(expression) {
                collect_grouping_expressions(part, grouping);
            }
        }
        PhysicalExpression::Binary { left, right, .. } => {
            collect_grouping_expressions(left, grouping);
            collect_grouping_expressions(right, grouping);
        }
        // Only the probe can carry an aggregate; the candidate list is
        // constant and never contributes a grouping key.
        PhysicalExpression::InList { expression, .. } => {
            collect_grouping_expressions(expression, grouping);
        }
        PhysicalExpression::Binding(_)
        | PhysicalExpression::Identity(_)
        | PhysicalExpression::Property { .. }
        | PhysicalExpression::Value(_)
        | PhysicalExpression::Aggregate { .. } => {}
    }
}

fn expression_requires_row(expression: &PhysicalExpression) -> bool {
    match expression {
        PhysicalExpression::Binding(_)
        | PhysicalExpression::Identity(_)
        | PhysicalExpression::Property { .. } => true,
        PhysicalExpression::Unary { expression, .. } => expression_requires_row(expression),
        PhysicalExpression::Function { arguments, .. } | PhysicalExpression::List(arguments) => {
            arguments.iter().any(expression_requires_row)
        }
        PhysicalExpression::Case { .. } => case_parts(expression)
            .into_iter()
            .any(expression_requires_row),
        PhysicalExpression::Binary { left, right, .. } => {
            expression_requires_row(left) || expression_requires_row(right)
        }
        // The candidate list is constant, so only the tested expression can
        // make an `IN` row-dependent.
        PhysicalExpression::InList { expression, .. } => expression_requires_row(expression),
        PhysicalExpression::Value(_) | PhysicalExpression::Aggregate { .. } => false,
    }
}

fn evaluate_group_expression(
    expression: &PhysicalExpression,
    rows: &[&BindingRow],
) -> EngineResult<QueryValue> {
    match expression {
        PhysicalExpression::Aggregate {
            function,
            distinct,
            expression,
        } => evaluate_aggregate(*function, *distinct, expression.as_deref(), rows),
        PhysicalExpression::Unary {
            operator,
            expression,
        } => {
            let value = evaluate_group_expression(expression, rows)?;
            evaluate_unary(*operator, query_value_to_evaluated(value)?).map(to_query_value)
        }
        PhysicalExpression::Binary {
            left,
            operator,
            right,
        } => {
            let left = evaluate_group_expression(left, rows)?;
            let right = evaluate_group_expression(right, rows)?;
            evaluate_binary(
                *operator,
                query_value_to_evaluated(left)?,
                query_value_to_evaluated(right)?,
            )
            .map(to_query_value)
        }
        PhysicalExpression::InList { expression, values } => {
            let value = query_value_to_evaluated(evaluate_group_expression(expression, rows)?)?;
            Ok(to_query_value(evaluate_in_list(value, values)))
        }
        // Each argument is evaluated over the group the way it would be on its
        // own -- an aggregate over the group, a grouping key from its first
        // row, a constant without one -- and the call applies to the results.
        // So `coalesce(max(x), 0)` reads the group, and a constant call still
        // answers when the group is empty.
        PhysicalExpression::Function {
            function,
            arguments,
        } => {
            let arguments = arguments
                .iter()
                .map(|argument| evaluate_group_expression(argument, rows))
                .collect::<EngineResult<Vec<_>>>()?;
            if *function == ScalarFunction::Coalesce {
                // A collected list (including an empty one) is a non-null value.
                // Do not force group results through scalar operand conversion.
                // Count is an internal aggregate tag; a scalar call exposes
                // its numeric value just like an integer literal or property.
                let selected = arguments
                    .into_iter()
                    .find(|v| {
                        !matches!(v, QueryValue::Null | QueryValue::Scalar(ScalarValue::Null))
                    })
                    .unwrap_or(QueryValue::Null);
                return Ok(match selected {
                    QueryValue::Count(count) => {
                        QueryValue::Scalar(ScalarValue::Integer(i128::from(count)))
                    }
                    value => value,
                });
            }
            let arguments = arguments
                .into_iter()
                .map(query_value_to_evaluated)
                .collect::<EngineResult<Vec<_>>>()?;
            evaluate_function(*function, arguments).map(to_query_value)
        }
        PhysicalExpression::List(items) => Ok(QueryValue::List(
            items
                .iter()
                .map(|item| evaluate_group_expression(item, rows))
                .collect::<EngineResult<Vec<_>>>()?,
        )),
        // A CASE wrapping an aggregate: each part is read over the group as
        // it would be alone, and the branch is chosen from those values.
        PhysicalExpression::Case { .. } if expression_contains_aggregate(expression) => {
            let mut evaluate = |part: &PhysicalExpression| {
                evaluate_group_expression(part, rows).and_then(query_value_to_evaluated)
            };
            evaluate_case(expression, &mut evaluate).map(to_query_value)
        }
        PhysicalExpression::Value(_) => {
            evaluate_expression(expression, &BindingRow::default()).map(to_query_value)
        }
        _ => rows
            .first()
            .ok_or_else(|| {
                unsupported(
                    QueryFailureReason::Evaluation,
                    "grouping expression has no input row",
                )
            })
            .and_then(|row| evaluate_expression(expression, row))
            .map(to_query_value),
    }
}

fn evaluate_aggregate(
    function: AggregateFunction,
    distinct: bool,
    expression: Option<&PhysicalExpression>,
    rows: &[&BindingRow],
) -> EngineResult<QueryValue> {
    let mut values = match expression {
        Some(expression) => rows
            .iter()
            .map(|row| evaluate_expression(expression, row).map(to_query_value))
            .collect::<EngineResult<Vec<_>>>()?,
        None => Vec::new(),
    };
    if distinct {
        // Borrow inputs while identifying first occurrences: duplicate values
        // are never cloned and the hash table grows with unique values only.
        let mut seen = std::collections::HashSet::new();
        let keep: Vec<bool> = values
            .iter()
            .map(|value| contains_nan(value) || seen.insert(AggregateDistinctKey(value)))
            .collect();
        drop(seen);
        values = values
            .into_iter()
            .zip(keep)
            .filter_map(|(value, keep)| keep.then_some(value))
            .collect();
    }
    match function {
        AggregateFunction::Count => Ok(QueryValue::Count(match expression {
            None => rows.len() as u64,
            Some(_) => values
                .iter()
                .filter(|value| !matches!(value, QueryValue::Null))
                .count() as u64,
        })),
        AggregateFunction::Collect => Ok(QueryValue::List(
            values
                .into_iter()
                .filter(|value| !matches!(value, QueryValue::Null))
                .collect(),
        )),
        AggregateFunction::Sum | AggregateFunction::Average => aggregate_numeric(function, &values),
        AggregateFunction::Min | AggregateFunction::Max => {
            let present = values
                .into_iter()
                .filter(|value| !matches!(value, QueryValue::Null));
            let extreme = if function == AggregateFunction::Min {
                present.min_by(compare_query_values)
            } else {
                present.max_by(compare_query_values)
            };
            Ok(extreme.unwrap_or(QueryValue::Null))
        }
    }
}

/// Numeric equality is semantic (1 == 1.0), recursively inside lists.
#[derive(Eq)]
struct AggregateDistinctKey<'a>(&'a QueryValue);

impl PartialEq for AggregateDistinctKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self.0, other.0) {
            (
                QueryValue::Null | QueryValue::Scalar(ScalarValue::Null),
                QueryValue::Null | QueryValue::Scalar(ScalarValue::Null),
            ) => true,
            (QueryValue::Count(a), QueryValue::Scalar(b))
            | (QueryValue::Scalar(b), QueryValue::Count(a)) => {
                scalar_values_equal(&ScalarValue::Integer(i128::from(*a)), b)
            }
            (
                QueryValue::Scalar(ScalarValue::Float(a)),
                QueryValue::Scalar(ScalarValue::Float(b)),
            ) => match (a.parse::<f64>(), b.parse::<f64>()) {
                (Ok(a), Ok(b)) => a == b || (a.is_nan() && b.is_nan()),
                _ => a == b,
            },
            (QueryValue::Scalar(a), QueryValue::Scalar(b)) => scalar_values_equal(a, b),
            (QueryValue::List(a), QueryValue::List(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b)
                        .all(|(a, b)| AggregateDistinctKey(a) == AggregateDistinctKey(b))
            }
            _ => self.0 == other.0,
        }
    }
}

impl std::hash::Hash for AggregateDistinctKey<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self.0 {
            QueryValue::Count(value) => {
                0_u8.hash(state);
                i128::from(*value).hash(state);
            }
            QueryValue::Null | QueryValue::Scalar(ScalarValue::Null) => {
                5_u8.hash(state);
            }
            QueryValue::Scalar(ScalarValue::Integer(value)) => {
                0_u8.hash(state);
                value.hash(state);
            }
            QueryValue::Scalar(ScalarValue::Float(value)) => {
                if let Ok(number) = value.parse::<f64>() {
                    // Only convert exactly representable integral floats. This
                    // avoids merging large distinct integers through f64 rounding.
                    if number.is_finite()
                        && number.fract() == 0.0
                        && (-170141183460469231731687303715884105728.0
                            ..170141183460469231731687303715884105728.0)
                            .contains(&number)
                    {
                        0_u8.hash(state);
                        (number as i128).hash(state);
                    } else {
                        1_u8.hash(state);
                        if number.is_nan() {
                            f64::NAN.to_bits()
                        } else {
                            number.to_bits()
                        }
                        .hash(state);
                    }
                } else {
                    2_u8.hash(state);
                    value.hash(state);
                }
            }
            QueryValue::List(values) => {
                3_u8.hash(state);
                values.len().hash(state);
                for value in values {
                    AggregateDistinctKey(value).hash(state);
                }
            }
            value => {
                4_u8.hash(state);
                value.hash(state);
            }
        }
    }
}

// NaN is unequal to itself in Cypher. Keep each occurrence in set/group
// operators while retaining reflexive equality for their Rust hash keys.
fn contains_nan(value: &QueryValue) -> bool {
    match value {
        QueryValue::Scalar(ScalarValue::Float(value)) => {
            value.parse::<f64>().is_ok_and(f64::is_nan)
        }
        QueryValue::List(values) => values.iter().any(contains_nan),
        _ => false,
    }
}
// Keying uses Cypher equality without changing QueryValue's typed result equality.
fn semantic_values_equal(left: &[QueryValue], right: &[QueryValue]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(a, b)| AggregateDistinctKey(a) == AggregateDistinctKey(b))
}
fn hash_semantic_values<H: std::hash::Hasher>(values: &[QueryValue], state: &mut H) {
    std::hash::Hash::hash(&values.len(), state);
    for value in values {
        std::hash::Hash::hash(&AggregateDistinctKey(value), state);
    }
}
#[derive(Eq)]
struct SemanticValues(Vec<QueryValue>, Option<usize>);
impl SemanticValues {
    fn new(values: Vec<QueryValue>, ordinal: usize) -> Self {
        let nonce = values.iter().any(contains_nan).then_some(ordinal);
        Self(values, nonce)
    }
}
impl PartialEq for SemanticValues {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1 && semantic_values_equal(&self.0, &other.0)
    }
}
impl std::hash::Hash for SemanticValues {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.1, state);
        hash_semantic_values(&self.0, state);
    }
}
#[derive(Eq)]
struct SemanticRow(QueryRow, Option<usize>);
impl SemanticRow {
    fn new(row: QueryRow, ordinal: usize) -> Self {
        let nonce = row.values.iter().any(contains_nan).then_some(ordinal);
        Self(row, nonce)
    }
}
impl PartialEq for SemanticRow {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1 && semantic_values_equal(&self.0.values, &other.0.values)
    }
}
impl std::hash::Hash for SemanticRow {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.1, state);
        hash_semantic_values(&self.0.values, state);
    }
}
#[derive(Eq)]
struct SemanticBindingRow(BindingRow, Option<usize>);
impl SemanticBindingRow {
    fn new(row: BindingRow, ordinal: usize) -> Self {
        let nonce = row.values.values().any(contains_nan).then_some(ordinal);
        Self(row, nonce)
    }
}
impl PartialEq for SemanticBindingRow {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
            && self.0.vertices == other.0.vertices
            && self.0.relationships == other.0.relationships
            && self.0.nulls == other.0.nulls
            && self.0.values.len() == other.0.values.len()
            && self.0.values.iter().all(|(name, value)| {
                other
                    .0
                    .values
                    .get(name)
                    .is_some_and(|other| AggregateDistinctKey(value) == AggregateDistinctKey(other))
            })
    }
}
impl std::hash::Hash for SemanticBindingRow {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.1, state);
        std::hash::Hash::hash(&self.0.vertices, state);
        std::hash::Hash::hash(&self.0.relationships, state);
        std::hash::Hash::hash(&self.0.nulls, state);
        std::hash::Hash::hash(&self.0.values.len(), state);
        for (name, value) in &self.0.values {
            std::hash::Hash::hash(name, state);
            std::hash::Hash::hash(&AggregateDistinctKey(value), state);
        }
    }
}

fn aggregate_numeric(
    function: AggregateFunction,
    values: &[QueryValue],
) -> EngineResult<QueryValue> {
    let mut integer_sum = 0_i128;
    let mut float_sum = 0_f64;
    let mut has_float = false;
    let mut count = 0_u64;
    for value in values {
        match value {
            QueryValue::Null => continue,
            QueryValue::Scalar(ScalarValue::Integer(value)) if !has_float => {
                integer_sum = integer_sum.checked_add(*value).ok_or_else(|| {
                    unsupported(
                        QueryFailureReason::Evaluation,
                        "aggregate integer sum overflowed",
                    )
                })?;
            }
            QueryValue::Scalar(ScalarValue::Integer(value)) => float_sum += *value as f64,
            QueryValue::Scalar(ScalarValue::Float(value)) => {
                if !has_float {
                    float_sum = integer_sum as f64;
                    has_float = true;
                }
                float_sum += value.parse::<f64>().map_err(|_| {
                    unsupported(
                        QueryFailureReason::Evaluation,
                        "aggregate contains an invalid float",
                    )
                })?;
            }
            _ => {
                return Err(unsupported(
                    QueryFailureReason::Evaluation,
                    "sum and avg require numeric values",
                ))
            }
        }
        count += 1;
    }
    // No numeric input: `avg` has no answer, but `sum` of nothing is zero, as
    // Neo4j answers it -- a total over an empty group is a number, not unknown.
    if count == 0 {
        return Ok(if function == AggregateFunction::Sum {
            QueryValue::Scalar(ScalarValue::Integer(0))
        } else {
            QueryValue::Null
        });
    }
    if function == AggregateFunction::Average {
        let total = if has_float {
            float_sum
        } else {
            integer_sum as f64
        };
        return Ok(QueryValue::Scalar(ScalarValue::Float(
            (total / count as f64).to_string().into(),
        )));
    }
    if has_float {
        Ok(QueryValue::Scalar(ScalarValue::Float(
            float_sum.to_string().into(),
        )))
    } else {
        Ok(QueryValue::Scalar(ScalarValue::Integer(integer_sum)))
    }
}

fn compare_projected_sort_keys(
    left: &[QueryValue],
    right: &[QueryValue],
    items: &[PhysicalSort],
) -> Ordering {
    for ((left, right), item) in left.iter().zip(right).zip(items) {
        let ordering = compare_query_values(left, right);
        let ordering = match item.direction {
            crate::SortDirection::Ascending => ordering,
            crate::SortDirection::Descending => ordering.reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compare_query_values(left: &QueryValue, right: &QueryValue) -> Ordering {
    match (left, right) {
        (QueryValue::Null, QueryValue::Null) => Ordering::Equal,
        (QueryValue::Null, _) => Ordering::Greater,
        (_, QueryValue::Null) => Ordering::Less,
        (QueryValue::VertexId(left), QueryValue::VertexId(right))
        | (QueryValue::RelationshipId(left), QueryValue::RelationshipId(right))
        | (QueryValue::Count(left), QueryValue::Count(right)) => left.cmp(right),
        (QueryValue::Scalar(left), QueryValue::Scalar(right)) => {
            compare_property_values(left, right)
        }
        (QueryValue::List(left), QueryValue::List(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_query_values(left, right))
            .find(|ordering| *ordering != Ordering::Equal)
            .unwrap_or_else(|| left.len().cmp(&right.len())),
        _ => query_value_rank(left).cmp(&query_value_rank(right)),
    }
}

fn query_value_rank(value: &QueryValue) -> u8 {
    match value {
        QueryValue::VertexId(_) => 0,
        QueryValue::RelationshipId(_) => 1,
        QueryValue::Count(_) => 2,
        QueryValue::Scalar(_) => 3,
        QueryValue::List(_) => 4,
        QueryValue::Null => u8::MAX,
    }
}

/// Index entries a relationship range probe may read before it declines to
/// its fallback expansion.
const MAX_RANGE_PROBE_ENTRIES: usize = 256;

/// Deliberately the same budget as the range probe. Both answer "is this
/// index narrow enough to read whole", and a type that needs a larger
/// allowance than a property range is a type the label anchor should keep.
const MAX_TYPE_PROBE_ENTRIES: usize = 256;

fn execute_binding_plan<'a>(
    read: &'a mut dyn GraphRead,
    plan: &'a GraphPhysicalPlan,
    row_limit: Option<usize>,
) -> Pin<Box<dyn Future<Output = EngineResult<Vec<BindingRow>>> + Send + 'a>> {
    execute_binding_plan_seeded(read, plan, row_limit, None)
}

fn execute_binding_plan_seeded<'a>(
    read: &'a mut dyn GraphRead,
    plan: &'a GraphPhysicalPlan,
    row_limit: Option<usize>,
    seed: Option<&'a BindingRow>,
) -> Pin<Box<dyn Future<Output = EngineResult<Vec<BindingRow>>> + Send + 'a>> {
    Box::pin(async move {
        read.operator_started(plan);
        let result = dispatch_binding_plan(&mut *read, plan, row_limit, seed).await;
        read.operator_finished(plan, result.as_ref().map_or(0, Vec::len));
        result
    })
}

fn dispatch_binding_plan<'a>(
    read: &'a mut dyn GraphRead,
    plan: &'a GraphPhysicalPlan,
    row_limit: Option<usize>,
    seed: Option<&'a BindingRow>,
) -> Pin<Box<dyn Future<Output = EngineResult<Vec<BindingRow>>> + Send + 'a>> {
    Box::pin(async move {
        if row_limit == Some(0) {
            return Ok(Vec::new());
        }
        read.checkpoint("cypher_execute_operator")?;
        match plan {
            // The one row that binds nothing, or the seed row a correlated
            // input already bound: either way exactly one.
            GraphPhysicalPlan::SingleRow => Ok(vec![seed.cloned().unwrap_or_default()]),
            GraphPhysicalPlan::Unwind {
                input,
                expression,
                binding,
            } => {
                // Every element of a row's list may be filtered or joined away
                // later, so the input runs unbounded; the output stops at the
                // row limit.
                // A nonempty constant list produces exactly this many rows per
                // input. Propagate only that proven bound; dependent lists can
                // be empty and must not lose later input rows.
                let input_limit = match expression {
                    PhysicalExpression::List(items)
                        if !items.is_empty()
                            && items
                                .iter()
                                .all(|item| matches!(item, PhysicalExpression::Value(_))) =>
                    {
                        row_limit.map(|limit| limit.div_ceil(items.len()))
                    }
                    _ => None,
                };
                let rows = execute_binding_plan_seeded(read, input, input_limit, seed).await?;
                let mut unwound = Vec::new();
                for row in rows {
                    read.checkpoint("cypher_unwind_rows")?;
                    let elements = match evaluate_expression(expression, &row)? {
                        EvaluatedValue::List(elements) => elements,
                        // Unwinding null yields nothing; any other single
                        // value is a list of one, as Neo4j treats it.
                        value if is_null(&value) => Vec::new(),
                        value => vec![value],
                    };
                    for element in elements {
                        let mut output = row.clone();
                        match element {
                            EvaluatedValue::Relationship(id) => {
                                let record = row
                                    .relationships
                                    .values()
                                    .chain(
                                        row.relationship_lists
                                            .values()
                                            .flat_map(|records| records.iter()),
                                    )
                                    .find(|record| record.id == id)
                                    .ok_or_else(|| {
                                        unsupported(
                                            QueryFailureReason::Unwind,
                                            "UNWIND relationship has no record in this row",
                                        )
                                    })?;
                                output.relationships.insert(binding.clone(), record.clone());
                            }
                            EvaluatedValue::Vertex(id) => {
                                let record = row
                                    .vertices
                                    .values()
                                    .chain(
                                        row.vertex_lists
                                            .values()
                                            .flat_map(|records| records.iter()),
                                    )
                                    .find(|record| record.id == id)
                                    .ok_or_else(|| {
                                        unsupported(
                                            QueryFailureReason::Unwind,
                                            "UNWIND vertex has no record in this row",
                                        )
                                    })?;
                                output.vertices.insert(binding.clone(), record.clone());
                            }
                            value => {
                                output.values.insert(binding.clone(), to_query_value(value));
                            }
                        }
                        unwound.push(output);
                        read.check_intermediate_rows(
                            "cypher_unwind_intermediate_rows",
                            unwound.len(),
                        )?;
                        if row_limit.is_some_and(|limit| unwound.len() >= limit) {
                            return Ok(unwound);
                        }
                    }
                }
                Ok(unwound)
            }
            GraphPhysicalPlan::Union { arms, all } => {
                let mut rows = Vec::new();
                for arm in arms {
                    // No row limit into an arm. For UNION the dedupe below can
                    // shrink the result, and even for UNION ALL the limit
                    // belongs to the combined result, not to each arm's share
                    // of it.
                    rows.extend(execute_binding_plan_seeded(read, arm, None, seed).await?);
                    read.check_intermediate_rows("cypher_union_intermediate_rows", rows.len())?;
                }
                if !*all {
                    // Insertion-ordered hash set, not a scan over what has been
                    // kept. `unique.contains` made this quadratic: at the
                    // default intermediate-row ceiling a UNION of distinct rows
                    // would compare billions of times, inside a loop that never
                    // reached a checkpoint, so neither the runtime budget nor a
                    // cancellation could end it.
                    let mut unique: IndexSet<SemanticBindingRow> =
                        IndexSet::with_capacity(rows.len());
                    for (ordinal, row) in rows.into_iter().enumerate() {
                        read.checkpoint("cypher_union_dedup")?;
                        unique.insert(SemanticBindingRow::new(row, ordinal));
                    }
                    rows = unique.into_iter().map(|row| row.0).collect();
                }
                Ok(rows)
            }
            GraphPhysicalPlan::VertexIdSeek {
                binding,
                labels,
                value,
            } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                let id = vertex_id_value(value)?;
                let rows = hydrate_rows(read, binding, labels, vec![id], row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            GraphPhysicalPlan::VertexPropertySeek {
                binding,
                labels,
                property,
                value,
            } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                let value = seek_value(value)?;
                let ids = read
                    .seek_vertices_by_property(property.as_str(), value)
                    .await?;
                let rows = hydrate_rows(read, binding, labels, ids, row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            GraphPhysicalPlan::VertexPropertyMultiSeek {
                binding,
                labels,
                property,
                values,
            } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                let unique_values = values
                    .iter()
                    .map(seek_value)
                    .collect::<EngineResult<BTreeSet<_>>>()?
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                // One batched request rather than one seek per value: the
                // storage adapter can overlap the index lookups, which is the
                // difference between N round trips and one on a remote store.
                let ids = read
                    .seek_vertices_by_property_values(property.as_str(), &unique_values)
                    .await?;
                let rows = hydrate_rows(read, binding, labels, ids, row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            GraphPhysicalPlan::VertexPropertyScan {
                binding,
                labels,
                property,
            } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                let ids = read.scan_vertices_by_property(property.as_str()).await?;
                let rows = hydrate_rows(read, binding, labels, ids, row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            GraphPhysicalPlan::OrderedVertexPropertyScan {
                binding,
                labels,
                property,
                direction,
                prefix,
                lower,
                upper,
                required,
                items,
                residual,
            } => {
                // A tie key the index stores is satisfied by arrival order, so
                // the walk can stop the moment it has enough rows. Any other
                // tie key reorders within a primary value, so the walk must
                // first read that value out -- and must not let the backend
                // drop any of it.
                let ties_by_other_key = items
                    .get(1)
                    .is_some_and(|item| !matches!(&item.expression, PhysicalExpression::Identity(tie) if tie == binding));
                let mut request = OrderedPropertyScanRequest {
                    property: property.to_string(),
                    labels: labels.iter().map(ToString::to_string).collect(),
                    ascending: *direction == SortDirection::Ascending,
                    // With no explicit tie key preserve the native scan's
                    // stable ID order, including descending primary scans.
                    id_ascending: items
                        .get(1)
                        .map_or(*direction == SortDirection::Ascending, |item| {
                            item.direction == SortDirection::Ascending
                        }),
                    prefix: prefix
                        .as_ref()
                        .map(|value| bound_string(value, "STARTS WITH"))
                        .transpose()?,
                    lower: lower
                        .as_ref()
                        .map(|(value, inclusive)| {
                            Ok::<_, EngineError>(PropertyBound {
                                value: seek_value(value)?.clone(),
                                inclusive: *inclusive,
                            })
                        })
                        .transpose()?,
                    upper: upper
                        .as_ref()
                        .map(|(value, inclusive)| {
                            Ok::<_, EngineError>(PropertyBound {
                                value: seek_value(value)?.clone(),
                                inclusive: *inclusive,
                            })
                        })
                        .transpose()?,
                    // The enclosing window fixed `required`; an outer limit can
                    // only make it smaller.
                    required: row_limit.map_or(*required, |limit| limit.min(*required)),
                    after: None,
                    // Both reasons the backend's own top-of-page selection
                    // would be wrong for this operator: it cannot see the
                    // residual that may reject everything it kept, and it
                    // orders ties by vertex ID rather than by the tie key
                    // this walk was allowed to carry.
                    index_order: residual.is_some() || ties_by_other_key,
                    allowed: None,
                    max_index_entries: None,
                };
                // Whether this walk has to read a primary value's whole run
                // before it can rank within it. An `index_order` page arrives
                // in the index's own order, which ranks ties by vertex ID in
                // the scan's direction; any other tie order -- another key, or
                // IDs the other way round -- needs the run read out first.
                // Retention stays bounded regardless: the walk keeps only the
                // best `target` survivors as it goes.
                let drains_ties = request.index_order
                    && (ties_by_other_key || request.id_ascending != request.ascending);
                // A correlated seed already holds the vertex: the removed
                // Filter must still be honored, so check it here instead of
                // trusting the label-only seed shortcut.
                if seed.is_some_and(|seed| seed.is_null(binding)) {
                    return Ok(Vec::new());
                }
                if let Some(vertex) = seed.and_then(|seed| seed.vertices.get(binding)) {
                    let accepted = labels
                        .iter()
                        .all(|label| vertex.labels.contains(label.as_str()))
                        && matches!(
                            vertex.properties.get(request.property.as_str()),
                            Some(value) if request.accepts(value)
                        )
                        && residual_accepts(residual.as_ref(), binding, vertex)?;
                    return Ok(accepted
                        .then(|| seed.cloned())
                        .flatten()
                        .into_iter()
                        .collect());
                }
                if request.required == 0 {
                    return Ok(Vec::new());
                }
                // A residual rejects rows the index happily returned, and a
                // rejected row does not count against `required`. So one page
                // can come back with too few survivors, and stopping there
                // would answer with fewer rows than the query asked for --
                // quietly, since nothing about it is an error. Keep asking.
                let target = request.required;
                let mut rows: Vec<BindingRow> = Vec::new();
                // While draining, `rows` is kept ranked by `items`, and these
                // are its sort keys, index for index.
                let mut ranked_keys: Vec<Vec<EvaluatedValue>> = Vec::new();
                let mut page = request.required;
                let mut examined = 0usize;
                let mut probed = false;
                loop {
                    request.required = page;
                    let scanned = read.scan_vertices_by_property_ordered(&request).await?;
                    let exhausted = scanned.exhausted;
                    request.after = scanned.last.clone();
                    let produced = scanned.vertices.len();
                    let kept_before = rows.len();
                    for vertex in scanned.vertices {
                        // Rows arrive in rank order unless the walk is
                        // draining, so once the window is full nothing later
                        // in the page can enter it.
                        if !drains_ties && rows.len() >= target {
                            break;
                        }
                        read.checkpoint("cypher_ordered_scan_vertex")?;
                        // Defensive re-check: a backend that returns a superset is
                        // still correct, one that returns a wrong row is not.
                        if !labels
                            .iter()
                            .all(|label| vertex.labels.contains(label.as_str()))
                            || !matches!(
                                vertex.properties.get(request.property.as_str()),
                                Some(value) if request.accepts(value)
                            )
                        {
                            continue;
                        }
                        // The scan replaced a Filter, so this is what keeps the
                        // result correct. A rejected row is not a row: it must not
                        // count against `required`, which is why the walk has to
                        // keep reading rather than stop at a full page.
                        if !residual_accepts(residual.as_ref(), binding, &vertex)? {
                            continue;
                        }
                        let row = BindingRow {
                            vertices: BTreeMap::from([(binding.clone(), vertex)]),
                            relationships: BTreeMap::new(),
                            nulls: BTreeSet::new(),
                            values: BTreeMap::new(),
                            vertex_lists: BTreeMap::new(),
                            relationship_lists: BTreeMap::new(),
                        };
                        // Draining a run ranks within it, so only the best
                        // `target` survivors can matter. Each one either takes
                        // its place among them or is dropped on arrival: the
                        // run can be the whole index, and even one row past the
                        // window would fail a window sized at the row budget.
                        if drains_ties {
                            insert_ranked(&mut rows, &mut ranked_keys, row, items, target)?;
                        } else {
                            rows.push(row);
                        }
                        read.check_intermediate_rows(
                            "cypher_ordered_scan_intermediate_rows",
                            rows.len(),
                        )?;
                    }
                    examined = examined.saturating_add(produced);
                    if exhausted || produced == 0 {
                        break;
                    }
                    if rows.len() >= target {
                        if !drains_ties
                            || tie_boundary_passed(
                                &rows,
                                binding,
                                request.property.as_str(),
                                target,
                                request.after.as_ref(),
                            )
                        {
                            break;
                        }
                        // Only rows still sharing the boundary value can change
                        // the answer now, and on a distinct property there are
                        // none. Ask for the smallest page rather than letting
                        // the miss-rate growth below widen one.
                        page = target;
                        continue;
                    }
                    // The walk has read far past what it was asked for and is
                    // still short, so the residual rather than the window is
                    // deciding this query. One of its equalities may name only
                    // a handful of vertices, and seeking those beats walking
                    // whatever is left. Ask the storage probe, which answers
                    // `None` once every equality overflows its cap -- the same
                    // evidence that no seek is narrow enough to be worth
                    // preferring. That answer cannot change as the walk goes
                    // on, so ask once.
                    if !probed && examined >= target.saturating_mul(ORDERED_PROBE_TRIGGER) {
                        probed = true;
                        if let Some(residual) = residual.as_ref() {
                            let candidates = equality_candidates(residual, binding);
                            if !candidates.is_empty() {
                                read.checkpoint("cypher_ordered_scan_probe")?;
                                if let Some(ids) =
                                    read.probe_selective_equality(&candidates).await?
                                {
                                    // A small seed is cheapest loaded whole and
                                    // sorted. A large one is not: keep walking
                                    // in order and let the backend skip every
                                    // entry outside it without loading it.
                                    if ids.len() > ORDERED_SEED_HYDRATE_MAX {
                                        let mut ids = ids;
                                        ids.sort_unstable();
                                        ids.dedup();
                                        request.allowed = Some(std::sync::Arc::new(ids));
                                    } else {
                                        // The probe answers for a whole conjunct,
                                        // so its vertices are a superset of every
                                        // row the residual can accept, including
                                        // the ones already collected. Answering
                                        // from it alone is therefore complete, and
                                        // simpler than reconciling two partial
                                        // walks of the same population.
                                        let hydrated = hydrate_rows(
                                            read,
                                            binding,
                                            labels,
                                            ids,
                                            None,
                                            Some(residual),
                                        )
                                        .await?;
                                        // Hydration applied the labels and the
                                        // residual; the window bounds the scan
                                        // would have enforced are still owed.
                                        let within_window = hydrated
                                            .into_iter()
                                            .filter(|row| {
                                                row.vertices.get(binding).is_some_and(|vertex| {
                                                    matches!(
                                                        vertex
                                                            .properties
                                                            .get(request.property.as_str()),
                                                        Some(value)
                                                            if request.accepts(value)
                                                    )
                                                })
                                            })
                                            .collect();
                                        let mut answer = sort_rows(within_window, items)?;
                                        truncate_to_limit(&mut answer, Some(target));
                                        return merge_seed_rows(answer, seed);
                                    }
                                }
                            }
                        }
                    }
                    // A window the residual barely matches would otherwise cost
                    // a round trip per surviving row. Grow with the miss rate,
                    // and cap it so a window it never matches cannot ask for
                    // the whole index in one page.
                    let kept = rows.len() - kept_before;
                    page = if kept == 0 {
                        page.saturating_mul(ORDERED_PAGE_GROWTH)
                    } else {
                        page.saturating_mul(produced).saturating_div(kept.max(1))
                    }
                    .clamp(target, target.saturating_mul(ORDERED_PAGE_CEILING));
                }
                let mut rows = sort_rows(rows, items)?;
                truncate_to_limit(&mut rows, Some(target));
                merge_seed_rows(rows, seed)
            }
            GraphPhysicalPlan::OrderedRelationshipPropertyScan {
                from,
                source_labels,
                relationship,
                relationship_type,
                direction,
                to,
                target_labels,
                property,
                sort_direction,
                prefix,
                lower,
                upper,
                required,
                items,
                residual,
            } => {
                // The planner only places this operator at the root of a
                // query body's window, which never runs correlated.
                if seed.is_some() {
                    return Err(unsupported(
                        QueryFailureReason::OrderWindow,
                        "an ordered relationship window cannot run under a correlated input",
                    ));
                }
                let bound = |bound: &Option<(BoundValue, bool)>| {
                    bound
                        .as_ref()
                        .map(|(value, inclusive)| {
                            Ok::<_, EngineError>(PropertyBound {
                                value: seek_value(value)?.clone(),
                                inclusive: *inclusive,
                            })
                        })
                        .transpose()
                };
                let request = OrderedRelationshipPropertyScanRequest {
                    relationship_type: relationship_type.to_string(),
                    property: property.to_string(),
                    ascending: *sort_direction == SortDirection::Ascending,
                    prefix: prefix
                        .as_ref()
                        .map(|value| bound_string(value, "STARTS WITH"))
                        .transpose()?,
                    lower: bound(lower)?,
                    upper: bound(upper)?,
                    required: row_limit.map_or(*required, |limit| limit.min(*required)),
                    after: None,
                };
                if request.required == 0 {
                    return Ok(Vec::new());
                }
                ordered_relationship_window_rows(
                    read,
                    request,
                    RelationshipWindow {
                        from,
                        source_labels,
                        relationship,
                        direction: *direction,
                        to,
                        target_labels,
                    },
                    items,
                    residual.as_ref(),
                )
                .await
            }
            GraphPhysicalPlan::VertexLabelScan {
                binding,
                label,
                labels,
            } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                // A limit cannot stop the anchor scan early when additional
                // labels still need in-memory validation: the first N anchor
                // rows may all fail the remaining labels.
                let scan_limit = (labels.len() == 1).then_some(row_limit).flatten();
                let ids = read
                    .scan_vertices_by_label(label.as_str(), scan_limit)
                    .await?;
                let rows = hydrate_rows(read, binding, labels, ids, row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            GraphPhysicalPlan::AllVertexScan { binding } => {
                if let Some(row) = bound_seed_row(seed, binding, &[]) {
                    return Ok(row);
                }
                read.record_full_scan();
                let ids = read.scan_all_vertices().await?;
                let rows = hydrate_rows(read, binding, &[], ids, row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            GraphPhysicalPlan::RelationshipPropertySeek {
                from,
                source_labels,
                relationship,
                relationship_type,
                property,
                value,
                direction,
                to,
                target_labels,
            } => {
                let value = seek_value(value)?;
                let relationships = read
                    .seek_relationships_by_property(
                        relationship_type.as_str(),
                        property.as_str(),
                        value,
                    )
                    .await?;
                relationship_seek_rows(
                    read,
                    relationships,
                    from,
                    source_labels,
                    relationship,
                    relationship_type,
                    *direction,
                    to,
                    target_labels,
                    row_limit,
                    seed,
                    &[],
                    SeekOrder::Identity,
                )
                .await
            }
            GraphPhysicalPlan::RelationshipPropertyMultiSeek {
                from,
                source_labels,
                relationship,
                relationship_type,
                property,
                values,
                direction,
                to,
                target_labels,
            } => {
                // Two parameters bound to one value seek it once.
                let unique_values = values
                    .iter()
                    .map(seek_value)
                    .collect::<EngineResult<BTreeSet<_>>>()?
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                // A relationship matched by two values -- `1` and `1.0`, or a
                // backend that reports it per value -- comes back twice;
                // `relationship_seek_rows` keys by relationship identity, so
                // it is one row.
                let relationships = read
                    .seek_relationships_by_property_values(
                        relationship_type.as_str(),
                        property.as_str(),
                        &unique_values,
                    )
                    .await?;
                relationship_seek_rows(
                    read,
                    relationships,
                    from,
                    source_labels,
                    relationship,
                    relationship_type,
                    *direction,
                    to,
                    target_labels,
                    row_limit,
                    seed,
                    &[],
                    SeekOrder::Identity,
                )
                .await
            }
            GraphPhysicalPlan::RelationshipPropertyRangeProbe {
                from,
                source_labels,
                relationship,
                relationship_type,
                property,
                lower,
                inclusive,
                direction,
                to,
                target_labels,
                fallback,
            } => {
                let lower = seek_value(lower)?;
                match read
                    .probe_relationships_by_property_lower_bound(
                        relationship_type.as_str(),
                        property.as_str(),
                        lower,
                        *inclusive,
                        MAX_RANGE_PROBE_ENTRIES,
                    )
                    .await?
                {
                    Some(relationships) => {
                        relationship_seek_rows(
                            read,
                            relationships,
                            from,
                            source_labels,
                            relationship,
                            relationship_type,
                            *direction,
                            to,
                            target_labels,
                            row_limit,
                            seed,
                            &[],
                            SeekOrder::Expansion,
                        )
                        .await
                    }
                    None => execute_binding_plan_seeded(read, fallback, row_limit, seed).await,
                }
            }
            // Reached directly only without a Filter parent or under a seed:
            // the Filter arm runs these itself so it may probe after a
            // decline.
            GraphPhysicalPlan::VertexPropertyRangeSeek {
                binding,
                labels,
                fallback,
                ..
            }
            | GraphPhysicalPlan::VertexGuardedAccess {
                binding,
                labels,
                fallback,
                ..
            } => {
                // A seed that binds the vertex already fixes it.
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                // One that does not -- the right side of a join or an
                // OPTIONAL MATCH -- runs this once per outer row. The estimate
                // and read limit describe one run, not the total, and every
                // run would spend its reads again. The plan chosen without
                // statistics is what runs here today, so it is what runs here.
                if seed.is_some() {
                    return execute_binding_plan_seeded(read, fallback, row_limit, seed).await;
                }
                match statistics_access_rows(read, plan).await? {
                    Some(mut rows) => {
                        truncate_to_limit(&mut rows, row_limit);
                        Ok(rows)
                    }
                    // The access proved wider than statistics said. The
                    // fallback is the plan chosen without them, so a wrong
                    // estimate costs the entries read so far and no more.
                    None => execute_binding_plan_seeded(read, fallback, row_limit, None).await,
                }
            }
            GraphPhysicalPlan::RelationshipTypeProbe {
                from,
                source_labels,
                relationship,
                relationship_type,
                direction,
                to,
                target_labels,
                fallback,
            } => {
                // A seed that already binds the anchor has narrowed it to one
                // vertex, which is the case the probe exists to avoid needing.
                // Reading a whole type to serve one vertex is strictly worse
                // than expanding from it, and on a correlated input it would
                // repeat per outer row. The planner cannot see this: whether a
                // seed arrives is a property of execution, not of the plan.
                if seed.is_some_and(|row| row.vertices.contains_key(from) || row.is_null(from)) {
                    return execute_binding_plan_seeded(read, fallback, row_limit, seed).await;
                }
                match read
                    .probe_relationships_by_type(relationship_type.as_str(), MAX_TYPE_PROBE_ENTRIES)
                    .await?
                {
                    Some(relationships) => {
                        relationship_seek_rows(
                            read,
                            relationships,
                            from,
                            source_labels,
                            relationship,
                            relationship_type,
                            *direction,
                            to,
                            target_labels,
                            row_limit,
                            seed,
                            &[],
                            SeekOrder::Expansion,
                        )
                        .await
                    }
                    None => execute_binding_plan_seeded(read, fallback, row_limit, seed).await,
                }
            }
            GraphPhysicalPlan::Expand {
                input,
                from,
                relationship,
                relationship_types,
                direction,
                to,
                target_labels,
            } => {
                // An input row can expand to zero rows, so limiting the input
                // would be semantically incorrect. Bound the expansion output
                // instead.
                let rows = execute_binding_plan_seeded(read, input, None, seed).await?;
                expand_rows(
                    read,
                    rows,
                    ExpandSpec {
                        from,
                        relationship: relationship.as_ref(),
                        relationship_types,
                        direction: *direction,
                        to,
                        target_labels,
                    },
                    row_limit,
                    &[],
                )
                .await
            }
            GraphPhysicalPlan::VariableExpand {
                input,
                from,
                relationship,
                relationship_types,
                direction,
                to,
                target_labels,
                min_hops,
                max_hops,
            } => {
                let rows = execute_binding_plan_seeded(read, input, None, seed).await?;
                variable_expand_rows(
                    read,
                    rows,
                    from,
                    relationship.as_ref(),
                    relationship_types,
                    *direction,
                    to,
                    target_labels,
                    *min_hops,
                    *max_hops,
                    row_limit,
                    &[],
                )
                .await
            }
            GraphPhysicalPlan::NaturalJoin {
                left,
                right,
                optional,
            } => {
                let left_rows = execute_binding_plan_seeded(read, left, None, seed).await?;
                let mut joined = Vec::new();
                // Only an optional join needs these, and only once.
                let mut right_bindings: Option<BTreeSet<Symbol>> = None;
                for left_row in left_rows {
                    read.checkpoint("cypher_natural_join_left")?;
                    // Under a window the right side need produce no more than
                    // the rows still missing: its rows are appended whole, in
                    // order, so a prefix of them is a prefix of the join. An
                    // optional side still tells empty from not, since at
                    // least one row is always asked for.
                    let right_limit = row_limit.map(|limit| limit.saturating_sub(joined.len()));
                    let right_rows =
                        execute_binding_plan_seeded(read, right, right_limit, Some(&left_row))
                            .await?;
                    if *optional && right_rows.is_empty() {
                        // Null-pad rather than pass through unchanged: the
                        // names this side would have bound are null from here
                        // on, and a later required clause has to be able to
                        // tell that from a name nothing has produced yet.
                        let mut missed = left_row;
                        missed.mark_null(right_bindings.get_or_insert_with(|| right.bindings()));
                        joined.push(missed);
                    } else {
                        joined.extend(right_rows);
                    }
                    read.check_intermediate_rows("cypher_join_intermediate_rows", joined.len())?;
                    if let Some(limit) = row_limit {
                        if joined.len() >= limit {
                            joined.truncate(limit);
                            return Ok(joined);
                        }
                    }
                }
                Ok(joined)
            }
            GraphPhysicalPlan::Filter { input, predicate } => {
                // A window over a Filter over an expansion: run the hop with
                // the window and the predicate, so it stops reading
                // relationships once enough rows survive instead of
                // expanding every input whole for the Filter to discard most
                // of. Stacked Filters fold in, innermost first.
                if let Some(limit) = row_limit {
                    let mut folded = Vec::new();
                    let mut residuals = vec![predicate];
                    let mut below: &GraphPhysicalPlan = input;
                    while let GraphPhysicalPlan::Filter { input, predicate } = below {
                        folded.push(below);
                        residuals.push(predicate);
                        below = input;
                    }
                    if is_windowed_hop(below) {
                        residuals.reverse();
                        // This invocation is the outermost Filter, already
                        // bracketed; the inner ones folded in still report to
                        // the profiler as operators that ran.
                        for operator in &folded {
                            read.operator_started(operator);
                        }
                        let rows = windowed_hop_rows(read, below, limit, seed, &residuals).await;
                        let rows_out = rows.as_ref().map_or(0, Vec::len);
                        for operator in folded.iter().rev() {
                            read.operator_finished(operator, rows_out);
                        }
                        return rows;
                    }
                }
                let mut access: &GraphPhysicalPlan = input;
                // An access statistics chose runs without the probe: the
                // planner already ranked every equality, and probing would
                // spend the reads the plan saved. Its guard is its read
                // limit. Past it, the access declines to the plan chosen
                // without statistics, and that plan is probed below exactly
                // as it is today, so a wrong estimate is never worse than
                // having no statistics.
                if seed.is_none() {
                    if let Some(fallback) = statistics_access_fallback(input) {
                        read.operator_started(input);
                        let answered = statistics_access_rows(read, input).await;
                        let rows_out = answered
                            .as_ref()
                            .ok()
                            .and_then(Option::as_ref)
                            .map_or(0, Vec::len);
                        read.operator_finished(input, rows_out);
                        match answered? {
                            Some(rows) => return filter_rows(read, rows, predicate, row_limit),
                            None => access = fallback,
                        }
                    }
                }
                // The planner commits to an access path from predicate order
                // alone, so a broad property can anchor a query that a second,
                // narrow one would have answered directly. Offer the backend
                // every equality on the binding and let a bounded probe
                // decide. Correlated execution keeps the planned path: `seed`
                // already fixes the vertex, so there is nothing to narrow.
                if seed.is_none() {
                    if let Some((binding, labels)) = probe_replaceable_access(access) {
                        let candidates = equality_candidates(predicate, binding);
                        if distinct_properties(&candidates) >= 2 {
                            read.checkpoint("cypher_equality_probe")?;
                            if let Some(ids) = read.probe_selective_equality(&candidates).await? {
                                // The probe proved one conjunct covers these
                                // vertices. Labels and the whole predicate are
                                // still applied while they are hydrated, so
                                // this replaces the Filter rather than
                                // weakening it.
                                return hydrate_rows(
                                    read,
                                    binding,
                                    labels,
                                    ids,
                                    row_limit,
                                    Some(predicate),
                                )
                                .await;
                            }
                        }
                    }
                }
                // A filter may discard any prefix, so it must consume an
                // unbounded child. It can still stop materializing its own
                // output once a parent LIMIT is satisfied.
                let rows = execute_binding_plan_seeded(read, access, None, seed).await?;
                filter_rows(read, rows, predicate, row_limit)
            }
            GraphPhysicalPlan::Sort { input, items, .. } => {
                // Sorting needs the complete input to identify the first row.
                let rows = execute_binding_plan_seeded(read, input, None, seed).await?;
                // The input is the largest collection this operator holds, and
                // it holds it whether or not a window narrows what it returns:
                // the child hands over a materialized `Vec`. Reported here, at
                // that peak, so the counter states the retention that actually
                // happens rather than the window it returns -- bounding this
                // needs a child that streams, not a narrower parent.
                read.record_retained_rows(rows.len());
                let rows = match row_limit {
                    // Under a window only the leading `SKIP + LIMIT` rows can
                    // reach the parent, so return no more than that, ranked by
                    // a heap that holds at most that many.
                    Some(limit) => top_k_rows(rows, items, limit)?,
                    None => sort_rows(rows, items)?,
                };
                read.check_intermediate_rows("cypher_sort_intermediate_rows", rows.len())?;
                Ok(rows)
            }
            GraphPhysicalPlan::Skip { input, count } => {
                let count = window_count(count, "SKIP")?;
                let child_limit = row_limit
                    .map(|limit| {
                        count.checked_add(limit).ok_or_else(|| {
                            unsupported(
                                QueryFailureReason::OrderWindow,
                                "SKIP plus LIMIT exceeds the supported usize range",
                            )
                        })
                    })
                    .transpose()?;
                let rows = execute_binding_plan_seeded(read, input, child_limit, seed).await?;
                let mut rows = rows.into_iter().skip(count).collect::<Vec<_>>();
                truncate_to_limit(&mut rows, row_limit);
                read.check_intermediate_rows("cypher_skip_intermediate_rows", rows.len())?;
                Ok(rows)
            }
            GraphPhysicalPlan::Limit { input, count } => {
                let count = window_count(count, "LIMIT")?;
                let effective_limit = Some(row_limit.map_or(count, |outer| outer.min(count)));
                let mut rows =
                    execute_binding_plan_seeded(read, input, effective_limit, seed).await?;
                truncate_to_limit(&mut rows, effective_limit);
                read.check_intermediate_rows("cypher_limit_intermediate_rows", rows.len())?;
                Ok(rows)
            }
            GraphPhysicalPlan::Project {
                input,
                items,
                distinct,
                post_sort,
                post_skip,
                post_limit,
            } => {
                // A projection below a result plan is a scope boundary: it
                // replaces the bindings in flight rather than adding to them,
                // which is what makes `WITH` a boundary rather than a comment.
                //
                // Without DISTINCT, grouping or a sort it maps rows one to
                // one, so a window over it -- its own or a parent's -- needs
                // only that many input rows past its own SKIP.
                let one_to_one = !*distinct
                    && post_sort.is_empty()
                    && !items
                        .iter()
                        .any(|item| expression_contains_aggregate(&item.expression));
                let input_limit = if one_to_one {
                    let own_limit = post_limit
                        .as_ref()
                        .map(|count| window_count(count, "LIMIT"))
                        .transpose()?;
                    let window = match (row_limit, own_limit) {
                        (Some(outer), Some(own)) => Some(outer.min(own)),
                        (window, None) | (None, window) => window,
                    };
                    let skip = post_skip
                        .as_ref()
                        .map(|count| window_count(count, "SKIP"))
                        .transpose()?
                        .unwrap_or(0);
                    window.and_then(|window| window.checked_add(skip))
                } else {
                    None
                };
                let input_rows =
                    execute_binding_plan_seeded(read, input, input_limit, seed).await?;
                project_binding_rows(
                    read, input_rows, items, *distinct, post_sort, post_skip, post_limit,
                )
            }
        }
    })
}

/// Whether a window over a Filter stack may be run inside `plan` by
/// [`windowed_hop_rows`] rather than over its whole output.
fn is_windowed_hop(plan: &GraphPhysicalPlan) -> bool {
    matches!(
        plan,
        GraphPhysicalPlan::Expand { .. }
            | GraphPhysicalPlan::VariableExpand { .. }
            | GraphPhysicalPlan::RelationshipTypeProbe { .. }
            | GraphPhysicalPlan::RelationshipPropertyRangeProbe { .. }
    )
}

/// A hop under a window and the predicates of the Filters above it
/// (innermost first; see [`residuals_hold`]): the leading `limit` rows of the
/// hop that pass them all, in the hop's own order, read without finishing the
/// hop once they are found.
///
/// A plain Filter must consume its input whole because it may discard any
/// prefix. Pushing its predicate into the hop instead lets the hop count only
/// surviving rows against the window. A probe that answers applies them to
/// what it read; one that declines hands them, with the window, to its
/// fallback expansion.
fn windowed_hop_rows<'a>(
    read: &'a mut dyn GraphRead,
    plan: &'a GraphPhysicalPlan,
    limit: usize,
    seed: Option<&'a BindingRow>,
    residuals: &'a [&'a PhysicalExpression],
) -> Pin<Box<dyn Future<Output = EngineResult<Vec<BindingRow>>> + Send + 'a>> {
    Box::pin(async move {
        if !is_windowed_hop(plan) {
            // A probe's fallback that is not itself a hop: run it whole, as
            // its own operator, and filter it as the Filter stack would have.
            let rows = execute_binding_plan_seeded(read, plan, None, seed).await?;
            let mut kept = Vec::new();
            for row in rows {
                read.checkpoint("cypher_filter_rows")?;
                if residuals_hold(&row, residuals)? {
                    kept.push(row);
                    if kept.len() >= limit {
                        break;
                    }
                }
            }
            return Ok(kept);
        }
        read.operator_started(plan);
        let rows = windowed_hop_dispatch(read, plan, limit, seed, residuals).await;
        read.operator_finished(plan, rows.as_ref().map_or(0, Vec::len));
        rows
    })
}

async fn windowed_hop_dispatch<'a>(
    read: &'a mut dyn GraphRead,
    plan: &'a GraphPhysicalPlan,
    limit: usize,
    seed: Option<&'a BindingRow>,
    residuals: &'a [&'a PhysicalExpression],
) -> EngineResult<Vec<BindingRow>> {
    read.checkpoint("cypher_execute_operator")?;
    match plan {
        GraphPhysicalPlan::Expand {
            input,
            from,
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
        } => {
            let rows = execute_binding_plan_seeded(read, input, None, seed).await?;
            let spec = ExpandSpec {
                from,
                relationship: relationship.as_ref(),
                relationship_types,
                direction: *direction,
                to,
                target_labels,
            };
            expand_rows(read, rows, spec, Some(limit), residuals).await
        }
        GraphPhysicalPlan::VariableExpand {
            input,
            from,
            relationship,
            relationship_types,
            direction,
            to,
            target_labels,
            min_hops,
            max_hops,
        } => {
            let rows = execute_binding_plan_seeded(read, input, None, seed).await?;
            variable_expand_rows(
                read,
                rows,
                from,
                relationship.as_ref(),
                relationship_types,
                *direction,
                to,
                target_labels,
                *min_hops,
                *max_hops,
                Some(limit),
                residuals,
            )
            .await
        }
        GraphPhysicalPlan::RelationshipTypeProbe {
            from,
            source_labels,
            relationship,
            relationship_type,
            direction,
            to,
            target_labels,
            fallback,
        } => {
            // As in the probe's own arm: a seed binding the anchor goes
            // straight to the expansion.
            if seed.is_some_and(|row| row.vertices.contains_key(from) || row.is_null(from)) {
                return windowed_hop_rows(read, fallback, limit, seed, residuals).await;
            }
            match read
                .probe_relationships_by_type(relationship_type.as_str(), MAX_TYPE_PROBE_ENTRIES)
                .await?
            {
                Some(relationships) => {
                    relationship_seek_rows(
                        read,
                        relationships,
                        from,
                        source_labels,
                        relationship,
                        relationship_type,
                        *direction,
                        to,
                        target_labels,
                        Some(limit),
                        seed,
                        residuals,
                        SeekOrder::Expansion,
                    )
                    .await
                }
                None => windowed_hop_rows(read, fallback, limit, seed, residuals).await,
            }
        }
        GraphPhysicalPlan::RelationshipPropertyRangeProbe {
            from,
            source_labels,
            relationship,
            relationship_type,
            property,
            lower,
            inclusive,
            direction,
            to,
            target_labels,
            fallback,
        } => {
            let lower = seek_value(lower)?;
            match read
                .probe_relationships_by_property_lower_bound(
                    relationship_type.as_str(),
                    property.as_str(),
                    lower,
                    *inclusive,
                    MAX_RANGE_PROBE_ENTRIES,
                )
                .await?
            {
                Some(relationships) => {
                    relationship_seek_rows(
                        read,
                        relationships,
                        from,
                        source_labels,
                        relationship,
                        relationship_type,
                        *direction,
                        to,
                        target_labels,
                        Some(limit),
                        seed,
                        residuals,
                        SeekOrder::Expansion,
                    )
                    .await
                }
                None => windowed_hop_rows(read, fallback, limit, seed, residuals).await,
            }
        }
        // `windowed_hop_rows` sends anything else down its filtered path.
        other => Err(unsupported(
            QueryFailureReason::Other,
            format!(
                "{} is not a hop a window can run inside",
                other.operator_name()
            ),
        )),
    }
}

/// How [`relationship_seek_rows`] orders what it read.
#[derive(Clone, Copy)]
enum SeekOrder {
    /// By relationship identity: an index seek's own order.
    Identity,
    /// As the expansion a probe stands in for emits them: by the `from`
    /// vertex, then the `to` vertex, then relationship identity. The probe's
    /// fallback expands a vertex scan, which arrives in vertex order, so a
    /// probe that answers and one that declines return one order -- which
    /// matters wherever arrival order survives, as it does for a tie between
    /// rows binding more than one relationship (see [`row_identity`]).
    Expansion,
}

/// Rows for the relationships an index or probe read, in `order`, keeping
/// only those that pass `residuals` (see [`residuals_hold`]) and stopping at
/// `row_limit` of them.
#[allow(clippy::too_many_arguments)]
async fn relationship_seek_rows(
    read: &mut dyn GraphRead,
    relationships: Vec<RelationshipRecord>,
    from: &Symbol,
    source_labels: &[Symbol],
    relationship_binding: &Symbol,
    relationship_type: &Symbol,
    direction: PatternDirection,
    to: &Symbol,
    target_labels: &[Symbol],
    row_limit: Option<usize>,
    seed: Option<&BindingRow>,
    residuals: &[&PhysicalExpression],
    order: SeekOrder,
) -> EngineResult<Vec<BindingRow>> {
    let relationships = canonical_relationships(read, relationships)?;
    let vertex_ids = relationships
        .values()
        .flat_map(|relationship| [relationship.source, relationship.target])
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let vertices = read
        .hydrate_vertices(&vertex_ids)
        .await?
        .into_iter()
        .map(|vertex| (vertex.id, vertex))
        .collect::<BTreeMap<_, _>>();
    let mut candidates = Vec::new();
    for relationship in relationships.values() {
        read.checkpoint("cypher_relationship_property_seek")?;
        if relationship.relationship_type != relationship_type.as_str() {
            continue;
        }
        // Neither endpoint is bound on this path, so an undirected hop matches
        // the edge under both assignments and yields both rows. A self-loop is
        // the exception: the two assignments are the same row.
        let orientations: &[(VertexId, VertexId)] = match direction {
            PatternDirection::Outgoing => &[(relationship.source, relationship.target)],
            PatternDirection::Incoming => &[(relationship.target, relationship.source)],
            PatternDirection::Undirected if relationship.source == relationship.target => {
                &[(relationship.source, relationship.target)]
            }
            PatternDirection::Undirected => &[
                (relationship.source, relationship.target),
                (relationship.target, relationship.source),
            ],
        };
        for &(source_id, target_id) in orientations {
            candidates.push((source_id, target_id, relationship));
        }
    }
    if matches!(order, SeekOrder::Expansion) {
        candidates
            .sort_by_key(|(source, target, relationship)| (*source, *target, relationship.id));
    }
    let mut rows = Vec::new();
    for (source_id, target_id, relationship) in candidates {
        read.checkpoint("cypher_relationship_property_seek")?;
        let (Some(source), Some(target)) = (vertices.get(&source_id), vertices.get(&target_id))
        else {
            continue;
        };
        if !source_labels
            .iter()
            .all(|label| source.labels.contains(label.as_str()))
            || !target_labels
                .iter()
                .all(|label| target.labels.contains(label.as_str()))
        {
            continue;
        }
        let row = BindingRow {
            vertices: BTreeMap::from([
                (from.clone(), source.clone()),
                (to.clone(), target.clone()),
            ]),
            relationships: BTreeMap::from([(relationship_binding.clone(), relationship.clone())]),
            nulls: BTreeSet::new(),
            values: BTreeMap::new(),
            vertex_lists: BTreeMap::new(),
            relationship_lists: BTreeMap::new(),
        };
        let Some(row) = seed.map_or(Some(row.clone()), |seed| merge_compatible_rows(seed, &row))
        else {
            continue;
        };
        if !residuals_hold(&row, residuals)? {
            continue;
        }
        rows.push(row);
        read.check_intermediate_rows("cypher_relationship_property_seek_rows", rows.len())?;
        if row_limit.is_some_and(|limit| rows.len() >= limit) {
            break;
        }
    }
    Ok(rows)
}

/// The best `capacity` rows seen so far under the complete requested order,
/// kept sorted best-first. Rows arrive in index order, so before the window
/// fills a new row lands inside the trailing tie, and afterwards only a tie on
/// the boundary value can still displace the worst member: insertion is a
/// binary search plus a shift across that tie, never across the window.
///
/// `T` is the implicit final tie-break: the vertex ID for vertex walks, and
/// `(source, target, relationship ID)` for relationship walks.
struct OrderedRowWindow<'a, T = VertexId> {
    items: &'a [PhysicalSort],
    id_ascending: bool,
    capacity: usize,
    rows: Vec<(Vec<EvaluatedValue>, T, BindingRow)>,
}

impl<'a, T: Ord + Copy> OrderedRowWindow<'a, T> {
    fn new(items: &'a [PhysicalSort], id_ascending: bool, capacity: usize) -> Self {
        Self {
            items,
            id_ascending,
            capacity,
            rows: Vec::new(),
        }
    }

    fn is_full(&self) -> bool {
        self.rows.len() >= self.capacity
    }

    fn compare(&self, left: (&[EvaluatedValue], T), right: (&[EvaluatedValue], T)) -> Ordering {
        compare_sort_keys(left.0, right.0, self.items).then_with(|| {
            let ordering = left.1.cmp(&right.1);
            if self.id_ascending {
                ordering
            } else {
                ordering.reverse()
            }
        })
    }

    /// Offer one row. Returns whether it was retained.
    fn offer(&mut self, keys: Vec<EvaluatedValue>, id: T, row: BindingRow) -> bool {
        let position = self.rows.partition_point(|(existing, existing_id, _)| {
            self.compare((existing, *existing_id), (&keys, id)) != Ordering::Greater
        });
        if position >= self.capacity {
            return false;
        }
        self.rows.insert(position, (keys, id, row));
        if self.rows.len() > self.capacity {
            self.rows.pop();
        }
        true
    }

    fn into_rows(self) -> Vec<BindingRow> {
        self.rows.into_iter().map(|(_, _, row)| row).collect()
    }
}

/// Rows to ask for in the next page of an ordered walk. The backend hydrates
/// every row it returns, so a page sized to the whole window hydrates rows the
/// window no longer has room for. While the window fills, ask for the open
/// slots scaled by the residual's observed selectivity, never fewer than the
/// open slots (legacy's batch size) and never more than the window. Before any
/// row has been kept there is no estimate, and a full window only needs its
/// boundary tie, which the narrowed bound already stops; both take the window.
fn continuation_page_size(capacity: usize, retained: usize, walked: usize, kept: usize) -> usize {
    let open = capacity.saturating_sub(retained);
    if open == 0 || kept == 0 {
        return capacity;
    }
    open.saturating_mul(walked)
        .div_ceil(kept)
        .clamp(open, capacity)
}

/// Drive a paged ordered index walk into a bounded window.
///
/// Each page arrives in index order. Every row is re-checked against labels
/// and the request's sound restrictions, then against the residual predicate,
/// and offered to the window. Once the window is full only rows tying the
/// worst member's leading value can still enter, so the walk stops at the
/// first row past that value; when the index already walks ties in the
/// requested order - vertex ID order, or a second property the backend ranked
/// each run by - it stops as soon as the window fills. Continuation pages
/// after the window fills are narrowed to the boundary value, so the backend
/// stops before hydrating anything beyond the tie.
struct RelationshipWindow<'a> {
    from: &'a Symbol,
    source_labels: &'a [Symbol],
    relationship: &'a Symbol,
    direction: PatternDirection,
    to: &'a Symbol,
    target_labels: &'a [Symbol],
}

/// Drive a paged ordered relationship property walk into a bounded window.
///
/// Rows arrive in `(value, source, target)` order. Each is re-checked against
/// the request, bound to the pattern (endpoints hydrated once per page, in one
/// batch, and label-checked), filtered by the residual and offered to the
/// window, whose final tie-break is `(source, target, relationship ID)` in the
/// walk direction. Parallel relationships may arrive in any order, and later
/// sort keys are evaluated on the row, so a full window is settled only when
/// the walk passes the boundary value: continuation pages are narrowed to it,
/// and the walk stops at the first row beyond it.
async fn ordered_relationship_window_rows(
    read: &mut dyn GraphRead,
    mut request: OrderedRelationshipPropertyScanRequest,
    pattern: RelationshipWindow<'_>,
    items: &[PhysicalSort],
    residual: Option<&PhysicalExpression>,
) -> EngineResult<Vec<BindingRow>> {
    let capacity = request.required;
    let ascending = request.ascending;
    let mut window =
        OrderedRowWindow::<(VertexId, VertexId, RelationshipId)>::new(items, ascending, capacity);
    let boundary = |window: &OrderedRowWindow<'_, (VertexId, VertexId, RelationshipId)>| {
        window
            .rows
            .last()
            .and_then(|(_, _, row)| row.relationships.get(pattern.relationship))
            .and_then(|relationship| relationship.properties.get(request.property.as_str()))
            .cloned()
    };
    let mut previous: Option<(ScalarValue, VertexId, VertexId)> = None;
    let mut walked = 0_usize;
    let mut kept = 0_usize;
    loop {
        let page = read
            .scan_relationships_by_property_ordered(&request)
            .await?;
        // Order check and index re-check first, so only rows that can still
        // matter have their endpoints hydrated.
        let mut candidates = Vec::with_capacity(page.relationships.len());
        for relationship in page.relationships {
            read.checkpoint("cypher_ordered_relationship_scan")?;
            let Some(value) = relationship
                .properties
                .get(request.property.as_str())
                .filter(|value| !matches!(value, ScalarValue::Null))
                .cloned()
            else {
                continue;
            };
            let position = (value, relationship.source, relationship.target);
            if let Some(previous) = &previous {
                let ordering = compare_property_values(&previous.0, &position.0)
                    .then_with(|| (previous.1, previous.2).cmp(&(position.1, position.2)));
                let ordering = if ascending {
                    ordering
                } else {
                    ordering.reverse()
                };
                if ordering.is_gt() {
                    return Err(unsupported(
                        QueryFailureReason::OrderWindow,
                        "ordered relationship property scan returned rows out of index order",
                    ));
                }
            }
            previous = Some(position.clone());
            if relationship.relationship_type != request.relationship_type
                || !request.accepts(&position.0)
            {
                continue;
            }
            candidates.push((position.0, relationship));
        }
        let endpoints = candidates
            .iter()
            .flat_map(|(_, relationship)| [relationship.source, relationship.target])
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let vertices = if endpoints.is_empty() {
            BTreeMap::new()
        } else {
            read.hydrate_vertices(&endpoints)
                .await?
                .into_iter()
                .map(|vertex| (vertex.id, vertex))
                .collect::<BTreeMap<_, _>>()
        };
        for (value, relationship) in candidates {
            read.checkpoint("cypher_ordered_relationship_scan_row")?;
            if window.is_full()
                && !boundary(&window)
                    .is_some_and(|boundary| compare_property_values(&boundary, &value).is_eq())
            {
                return Ok(window.into_rows());
            }
            let (from_id, to_id) = match pattern.direction {
                PatternDirection::Incoming => (relationship.target, relationship.source),
                PatternDirection::Outgoing | PatternDirection::Undirected => {
                    (relationship.source, relationship.target)
                }
            };
            let (Some(from), Some(to)) = (vertices.get(&from_id), vertices.get(&to_id)) else {
                continue;
            };
            if !pattern
                .source_labels
                .iter()
                .all(|label| from.labels.contains(label.as_str()))
                || !pattern
                    .target_labels
                    .iter()
                    .all(|label| to.labels.contains(label.as_str()))
            {
                continue;
            }
            let tie = (relationship.source, relationship.target, relationship.id);
            let row = BindingRow {
                vertices: BTreeMap::from([
                    (pattern.from.clone(), from.clone()),
                    (pattern.to.clone(), to.clone()),
                ]),
                relationships: BTreeMap::from([(pattern.relationship.clone(), relationship)]),
                // This walk binds vertices and a relationship, never a
                // projected value; `values` only carries what a WITH boundary
                // put there.
                values: BTreeMap::new(),
                vertex_lists: BTreeMap::new(),
                relationship_lists: BTreeMap::new(),
                // The walk only emits a hop it actually matched, so every name
                // this row carries is bound; none of them is a null an
                // OPTIONAL left behind.
                nulls: BTreeSet::new(),
            };
            walked += 1;
            if !residual_row_accepts(residual, &row)? {
                continue;
            }
            kept += 1;
            let keys = items
                .iter()
                .map(|item| evaluate_expression(&item.expression, &row))
                .collect::<EngineResult<Vec<_>>>()?;
            if window.offer(keys, tie, row) {
                read.check_intermediate_rows(
                    "cypher_ordered_relationship_scan_intermediate_rows",
                    window.rows.len(),
                )?;
            }
        }
        if page.exhausted {
            return Ok(window.into_rows());
        }
        let Some(next) = page.last else {
            return Ok(window.into_rows());
        };
        request.required = continuation_page_size(capacity, window.rows.len(), walked, kept);
        if window.is_full() {
            if let Some(boundary) = boundary(&window) {
                let bound = Some(PropertyBound {
                    value: boundary,
                    inclusive: true,
                });
                if ascending {
                    request.upper = bound;
                } else {
                    request.lower = bound;
                }
            }
        }
        request.after = Some(next);
    }
}

fn bound_seed_row(
    seed: Option<&BindingRow>,
    binding: &Symbol,
    labels: &[Symbol],
) -> Option<Vec<BindingRow>> {
    let seed = seed?;
    if seed.is_null(binding) {
        // An OPTIONAL bound this to null, so no vertex satisfies the pattern
        // and the row is eliminated. Returning `Some(no rows)` rather than
        // `None` is the whole point: `None` would fall through to a scan and
        // bind some unrelated vertex to a name Cypher says is null.
        return Some(Vec::new());
    }
    let vertex = seed.vertices.get(binding)?;
    Some(
        labels
            .iter()
            .all(|label| vertex.labels.contains(label.as_str()))
            .then(|| seed.clone())
            .into_iter()
            .collect(),
    )
}

fn merge_seed_rows(
    rows: Vec<BindingRow>,
    seed: Option<&BindingRow>,
) -> EngineResult<Vec<BindingRow>> {
    let Some(seed) = seed else {
        return Ok(rows);
    };
    Ok(rows
        .into_iter()
        .filter_map(|row| merge_compatible_rows(seed, &row))
        .collect())
}

fn truncate_to_limit<T>(rows: &mut Vec<T>, row_limit: Option<usize>) {
    if let Some(limit) = row_limit {
        rows.truncate(limit);
    }
}

/// Insert `row` into `rows`, which holds at most `limit` rows ranked by
/// `items`, with `keys` holding their sort keys index for index.
///
/// Equal keys keep arrival order and a row that ties the worst held one is
/// dropped, so the result is exactly what a stable sort of every row followed
/// by truncation to `limit` would keep -- without ever holding more than
/// `limit` rows.
fn insert_ranked(
    rows: &mut Vec<BindingRow>,
    keys: &mut Vec<Vec<EvaluatedValue>>,
    row: BindingRow,
    items: &[PhysicalSort],
    limit: usize,
) -> EngineResult<()> {
    let key = items
        .iter()
        .map(|item| evaluate_expression(&item.expression, &row))
        .collect::<EngineResult<Vec<_>>>()?;
    let at = keys.partition_point(|held| compare_sort_keys(held, &key, items) != Ordering::Greater);
    if at >= limit {
        return Ok(());
    }
    if rows.len() == limit {
        rows.pop();
        keys.pop();
    }
    rows.insert(at, row);
    keys.insert(at, key);
    Ok(())
}

fn sort_rows(mut rows: Vec<BindingRow>, items: &[PhysicalSort]) -> EngineResult<Vec<BindingRow>> {
    let keys = rows
        .iter()
        .map(|row| {
            items
                .iter()
                .map(|item| evaluate_expression(&item.expression, row))
                .collect::<EngineResult<Vec<_>>>()
        })
        .collect::<EngineResult<Vec<_>>>()?;
    let ranked = ranks_by_identity(&rows);
    let mut keyed = rows
        .drain(..)
        .zip(keys)
        .map(|(row, keys)| {
            let identity = ranked.then(|| row_identity(&row)).flatten();
            (row, keys, identity)
        })
        .collect::<Vec<_>>();
    keyed.sort_by(|(_, left, left_id), (_, right, right_id)| {
        compare_sort_keys(left, right, items).then_with(|| left_id.cmp(right_id))
    });
    Ok(keyed.into_iter().map(|(row, _, _)| row).collect())
}

/// The order a tie falls back to in a sort: the relationships a row binds, by
/// `(source, target, ID)`, in binding-name order, ascending.
///
/// This repairs one artifact and nothing else. Expansion groups its output by
/// the end the pattern binds first, so `(b)<-[r]-(a)` arrives in
/// `(target, source)` order where `(a)-[r]->(b)` arrives in
/// `(source, target)` order -- the same relationships, described from the
/// other end -- and a stable sort keeps whichever order it was handed, so a
/// tie ranked differently for the two spellings. Legacy ranks such a tie by
/// the relationship's own key either way, and it stays ascending whichever way
/// the sort keys run, because that is what a stable sort over an ascending
/// expansion does.
///
/// Whether this input is the shape [`row_identity`] was reasoned about: every
/// row binding exactly one relationship.
///
/// Decided once for the whole sort, and that is what makes the comparison a
/// total order. `None` sorts before `Some`, so a mixed input -- an
/// `OPTIONAL MATCH` hop bound in some rows and not others -- would rank the
/// two-relationship rows ahead of the rest whatever their arrival order, and
/// comparing identities only when both sides have one is worse than useless:
/// `Some(a)` and `Some(b)` would order while each compared `Equal` to a `None`
/// between them, which is not transitive and which `sort_by` is entitled to
/// answer with garbage.
fn ranks_by_identity(rows: &[BindingRow]) -> bool {
    rows.iter().all(|row| row.relationships.len() == 1)
}

/// The order a tie falls back to: the one relationship the row binds, by
/// `(source, target, ID)`, ascending. Only called when
/// [`ranks_by_identity`] holds.
///
/// Rows binding no relationship, or more than one, keep arrival order. Both
/// exclusions are deliberate.
///
/// A vertex scan has no grouping artifact to repair, and a sort can sit above
/// an ordered vertex walk that has already put a tie in the index's order --
/// re-ranking there would overwrite the answer the walk was asked for.
///
/// A multi-hop row binds several relationships, and the only order available
/// here is by binding name, which is not the order the hops ran in: with
/// `(a)-[z]->(b)-[k]->(c)` the second hop would lead. Expansion order is the
/// hop order, so leaving those rows to it is what keeps them matching the
/// plan this stands in for. The artifact being repaired is one expansion
/// grouping its output by the end its pattern binds first, which is a
/// single-hop shape.
fn row_identity(row: &BindingRow) -> Option<(VertexId, VertexId, RelationshipId)> {
    let mut relationships = row.relationships.values();
    match (relationships.next(), relationships.next()) {
        (Some(relationship), None) => {
            Some((relationship.source, relationship.target, relationship.id))
        }
        _ => None,
    }
}

/// The first `limit` rows of [`sort_rows`], ranked by a heap that holds at most
/// `limit` rows rather than by sorting the whole input. The input itself
/// arrives materialized and is consumed as the heap fills, so this bounds the
/// ranking structure and the result, not the operator's peak retention.
///
/// `sort_rows` ranks equal keys by [`row_identity`] and only then by arrival.
/// Ranking by `(keys, identity, input position)` reproduces that exactly:
/// a later row that ties the worst retained row ranks below it and is dropped,
/// so the boundary tie never needs extra slots and the result is identical to
/// sorting everything and truncating. A max-heap on that rank keeps the worst
/// retained row on top, so each input row costs `O(log limit)`.
fn top_k_rows(
    rows: Vec<BindingRow>,
    items: &[PhysicalSort],
    limit: usize,
) -> EngineResult<Vec<BindingRow>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let ranked = ranks_by_identity(&rows);
    let mut retained = BinaryHeap::with_capacity(limit.min(rows.len()));
    for (position, row) in rows.into_iter().enumerate() {
        let keys = items
            .iter()
            .map(|item| evaluate_expression(&item.expression, &row))
            .collect::<EngineResult<Vec<_>>>()?;
        let candidate = RankedRow {
            keys,
            identity: ranked.then(|| row_identity(&row)).flatten(),
            position,
            row,
            items,
        };
        if retained.len() < limit {
            retained.push(candidate);
        } else if let Some(mut worst) = retained.peek_mut() {
            if candidate < *worst {
                *worst = candidate;
            }
        }
    }
    Ok(retained
        .into_sorted_vec()
        .into_iter()
        .map(|ranked| ranked.row)
        .collect())
}

/// A row ranked by its sort keys, then by [`row_identity`], then by input
/// position (see [`top_k_rows`]).
struct RankedRow<'a> {
    keys: Vec<EvaluatedValue>,
    identity: Option<(VertexId, VertexId, RelationshipId)>,
    position: usize,
    row: BindingRow,
    items: &'a [PhysicalSort],
}

impl Ord for RankedRow<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_sort_keys(&self.keys, &other.keys, self.items)
            .then_with(|| self.identity.cmp(&other.identity))
            .then_with(|| self.position.cmp(&other.position))
    }
}

impl PartialOrd for RankedRow<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RankedRow<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for RankedRow<'_> {}

fn compare_sort_keys(
    left: &[EvaluatedValue],
    right: &[EvaluatedValue],
    items: &[PhysicalSort],
) -> Ordering {
    for ((left, right), item) in left.iter().zip(right).zip(items) {
        let ordering = compare_evaluated_values(left, right);
        let ordering = match item.direction {
            crate::SortDirection::Ascending => ordering,
            crate::SortDirection::Descending => ordering.reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compare_evaluated_values(left: &EvaluatedValue, right: &EvaluatedValue) -> Ordering {
    match (left, right) {
        (
            EvaluatedValue::Null | EvaluatedValue::Truth(None),
            EvaluatedValue::Null | EvaluatedValue::Truth(None),
        ) => Ordering::Equal,
        (EvaluatedValue::Null | EvaluatedValue::Truth(None), _) => Ordering::Greater,
        (_, EvaluatedValue::Null | EvaluatedValue::Truth(None)) => Ordering::Less,
        (EvaluatedValue::Vertex(left), EvaluatedValue::Vertex(right))
        | (EvaluatedValue::Relationship(left), EvaluatedValue::Relationship(right)) => {
            left.cmp(right)
        }
        (EvaluatedValue::Scalar(left), EvaluatedValue::Scalar(right)) => {
            compare_property_values(left, right)
        }
        (EvaluatedValue::Truth(Some(left)), EvaluatedValue::Truth(Some(right))) => left.cmp(right),
        (EvaluatedValue::List(left), EvaluatedValue::List(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_evaluated_values(left, right))
            .find(|ordering| *ordering != Ordering::Equal)
            .unwrap_or_else(|| left.len().cmp(&right.len())),
        _ => evaluated_value_rank(left).cmp(&evaluated_value_rank(right)),
    }
}

fn evaluated_value_rank(value: &EvaluatedValue) -> u8 {
    match value {
        EvaluatedValue::Truth(Some(_)) => 1,
        EvaluatedValue::Vertex(_) => 2,
        EvaluatedValue::Relationship(_) => 3,
        EvaluatedValue::List(_) => 0,
        EvaluatedValue::Scalar(value) => 4 + scalar_value_rank(value),
        EvaluatedValue::Null | EvaluatedValue::Truth(None) => u8::MAX,
    }
}

/// Entries one page of a range seek may read. Small enough that a decline is
/// noticed soon after the limit, large enough that a narrow range is one page.
const RANGE_SEEK_PAGE_ENTRIES: usize = 512;

/// The fields of a `VertexPropertyRangeSeek` the read needs.
struct RangeSeek<'a> {
    binding: &'a Symbol,
    labels: &'a [Symbol],
    property: &'a Symbol,
    lower: Option<&'a (BoundValue, bool)>,
    upper: Option<&'a (BoundValue, bool)>,
    estimated_rows: u64,
    read_limit: usize,
}

/// Read every vertex of `range.property` between the bounds that carries the
/// labels, or `None` once more than `read_limit` index entries have been read
/// without reaching the end of the range.
///
/// Rows are a candidate superset of what the enclosing Filter accepts: only
/// labels and the bounds are checked here, and the Filter applies the whole
/// predicate. They come back in vertex ID order, the order every other
/// vertex access hydrates in, so the access path cannot reorder results.
async fn range_seek_rows(
    read: &mut dyn GraphRead,
    range: RangeSeek<'_>,
) -> EngineResult<Option<Vec<BindingRow>>> {
    let bound = |bound: Option<&(BoundValue, bool)>| {
        bound
            .map(|(value, inclusive)| {
                Ok::<_, EngineError>(PropertyBound {
                    value: seek_value(value)?.clone(),
                    inclusive: *inclusive,
                })
            })
            .transpose()
    };
    let mut request = OrderedPropertyScanRequest {
        property: range.property.to_string(),
        labels: range.labels.iter().map(ToString::to_string).collect(),
        ascending: true,
        id_ascending: true,
        prefix: None,
        lower: bound(range.lower)?,
        upper: bound(range.upper)?,
        required: RANGE_SEEK_PAGE_ENTRIES,
        after: None,
        // Every entry is wanted, so the backend must not rank within a page.
        index_order: true,
        allowed: None,
        max_index_entries: None,
    };
    let limit = capped_read_limit(&*read, range.read_limit) as u64;
    let mut entries_read = 0_u64;
    let mut vertices: Vec<VertexRecord> = Vec::new();
    let declined = loop {
        // Under a cap of zero or one the seek may keep no entry, and even the
        // first would fail the row check before the guard declined, so it
        // declines before reading.
        if limit == 0 {
            break true;
        }
        // One entry past the limit is what proves the range is wider than it.
        let remaining = limit.saturating_add(1).saturating_sub(entries_read);
        let page = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .clamp(1, RANGE_SEEK_PAGE_ENTRIES);
        request.required = page;
        request.max_index_entries = Some(page);
        let scanned = read.scan_vertices_by_property_ordered(&request).await?;
        let page_entries = scanned
            .index_entries
            .unwrap_or(scanned.vertices.len() as u64);
        entries_read = entries_read.saturating_add(page_entries);
        for vertex in scanned.vertices {
            read.checkpoint("cypher_range_seek_vertex")?;
            // Defensive re-check, as for the ordered walk: a backend that
            // returns a superset is still correct, one that returns a wrong
            // row is not.
            if !range
                .labels
                .iter()
                .all(|label| vertex.labels.contains(label.as_str()))
                || !matches!(
                    vertex.properties.get(request.property.as_str()),
                    Some(value) if request.accepts(value)
                )
            {
                continue;
            }
            vertices.push(vertex);
            read.check_intermediate_rows("cypher_range_seek_intermediate_rows", vertices.len())?;
        }
        if scanned.exhausted {
            break false;
        }
        if entries_read > limit {
            break true;
        }
        // A page that neither read anything nor moved the cursor would be
        // asked for again forever. Declining is always safe.
        if page_entries == 0 || scanned.last.is_none() {
            break true;
        }
        request.after = scanned.last;
    };
    read.record_statistics_access(&StatisticsAccessReport {
        kind: AccessPathKind::Range,
        property: Some(range.property.to_string()),
        estimated_rows: range.estimated_rows,
        entries_read,
        declined,
    });
    if declined {
        return Ok(None);
    }
    vertices.sort_by_key(|vertex| vertex.id);
    vertices.dedup_by_key(|vertex| vertex.id);
    Ok(Some(
        vertices
            .into_iter()
            .map(|vertex| BindingRow {
                vertices: BTreeMap::from([(range.binding.clone(), vertex)]),
                relationships: BTreeMap::new(),
                nulls: BTreeSet::new(),
                values: BTreeMap::new(),
                vertex_lists: BTreeMap::new(),
                relationship_lists: BTreeMap::new(),
            })
            .collect(),
    ))
}

/// Keep the rows `predicate` accepts, stopping once `row_limit` are kept.
fn filter_rows(
    read: &mut dyn GraphRead,
    rows: Vec<BindingRow>,
    predicate: &PhysicalExpression,
    row_limit: Option<usize>,
) -> EngineResult<Vec<BindingRow>> {
    let mut filtered = Vec::new();
    for row in rows {
        read.checkpoint("cypher_filter_rows")?;
        if predicate_holds(
            evaluate_expression(predicate, &row)?,
            "Filter predicate did not evaluate to a boolean",
        )? {
            filtered.push(row);
            read.check_intermediate_rows("cypher_filter_intermediate_rows", filtered.len())?;
            if row_limit.is_some_and(|limit| filtered.len() >= limit) {
                break;
            }
        }
    }
    Ok(filtered)
}

/// The fallback of an access statistics chose, which declines to it at run
/// time once it reads past its guard.
fn statistics_access_fallback(plan: &GraphPhysicalPlan) -> Option<&GraphPhysicalPlan> {
    match plan {
        GraphPhysicalPlan::VertexPropertyRangeSeek { fallback, .. }
        | GraphPhysicalPlan::VertexGuardedAccess { fallback, .. } => Some(fallback),
        _ => None,
    }
}

/// Run an access [`statistics_access_fallback`] accepts: its rows, labels
/// checked and in vertex ID order, or `None` when it declined. Any other plan
/// declines without reading.
async fn statistics_access_rows(
    read: &mut dyn GraphRead,
    plan: &GraphPhysicalPlan,
) -> EngineResult<Option<Vec<BindingRow>>> {
    match plan {
        GraphPhysicalPlan::VertexPropertyRangeSeek {
            binding,
            labels,
            property,
            lower,
            upper,
            estimated_rows,
            read_limit,
            ..
        } => {
            let range = RangeSeek {
                binding,
                labels,
                property,
                lower: lower.as_ref(),
                upper: upper.as_ref(),
                estimated_rows: *estimated_rows,
                read_limit: *read_limit,
            };
            range_seek_rows(read, range).await
        }
        GraphPhysicalPlan::VertexGuardedAccess {
            binding,
            labels,
            access,
            estimated_rows,
            read_limit,
            ..
        } => guarded_rows(read, binding, labels, access, *estimated_rows, *read_limit).await,
        _ => Ok(None),
    }
}

/// Read a statistics-chosen equality seek or label scan, or `None` once it
/// holds more than `read_limit` index entries. The backend may decline
/// sooner, at a ceiling of its own; `None` is always safe.
///
/// Rows are a candidate superset, as for the range seek: labels are checked
/// here and the enclosing Filter applies the whole predicate.
/// A statistics-chosen access's read limit, kept under the query's index
/// candidate and intermediate row caps.
///
/// The access is an optional shortcut: reaching a cap proves only that it was
/// a poor choice, so it must decline to its fallback rather than fail a query
/// the fallback may answer. Its reads are not charged to the shared candidate
/// tally, and every row it keeps came from one entry it read, so holding its
/// entries under both caps holds its rows under them too. It reads one entry
/// past its limit to prove the access is wider, hence the `- 1`.
fn capped_read_limit(read: &dyn GraphRead, read_limit: usize) -> usize {
    [
        read.max_query_index_candidates(),
        read.max_intermediate_rows(),
    ]
    .into_iter()
    .flatten()
    .fold(read_limit, |limit, cap| limit.min(cap.saturating_sub(1)))
}

async fn guarded_rows(
    read: &mut dyn GraphRead,
    binding: &Symbol,
    labels: &[Symbol],
    access: &GuardedAccess,
    estimated_rows: u64,
    read_limit: usize,
) -> EngineResult<Option<Vec<BindingRow>>> {
    let read_limit = capped_read_limit(&*read, read_limit);
    let (kind, property, bounded) = match access {
        GuardedAccess::PropertySeek { property, values } => {
            let unique_values = values
                .iter()
                .map(seek_value)
                .collect::<EngineResult<BTreeSet<_>>>()?
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            let bounded = read
                .seek_vertices_by_property_values_bounded(
                    property.as_str(),
                    &unique_values,
                    read_limit,
                )
                .await?;
            (
                AccessPathKind::Equality,
                Some(property.to_string()),
                bounded,
            )
        }
        GuardedAccess::LabelScan { label } => {
            let bounded = read
                .scan_vertices_by_label_bounded(label.as_str(), read_limit)
                .await?;
            (AccessPathKind::LabelScan, None, bounded)
        }
    };
    read.record_statistics_access(&StatisticsAccessReport {
        kind,
        property,
        estimated_rows,
        entries_read: bounded.index_entries,
        declined: bounded.ids.is_none(),
    });
    let Some(ids) = bounded.ids else {
        return Ok(None);
    };
    hydrate_rows(read, binding, labels, ids, None, None)
        .await
        .map(Some)
}

/// The binding and required labels of a vertex access path whose candidate set
/// a runtime probe may replace.
///
/// A `VertexIdSeek` is already a point lookup and an
/// `OrderedVertexPropertyScan` owns its own bounded window, so neither has a
/// candidate set worth replacing.
fn probe_replaceable_access(plan: &GraphPhysicalPlan) -> Option<(&Symbol, &[Symbol])> {
    match plan {
        GraphPhysicalPlan::VertexPropertySeek {
            binding, labels, ..
        }
        | GraphPhysicalPlan::VertexPropertyMultiSeek {
            binding, labels, ..
        }
        | GraphPhysicalPlan::VertexPropertyScan {
            binding, labels, ..
        }
        | GraphPhysicalPlan::VertexLabelScan {
            binding, labels, ..
        } => Some((binding, labels)),
        GraphPhysicalPlan::AllVertexScan { binding } => Some((binding, &[])),
        _ => None,
    }
}

/// The equality restrictions on `binding` that are conjuncts of `predicate`.
///
/// Only conjuncts qualify. The vertices satisfying one conjunct are a superset
/// of the rows the whole predicate accepts, which is what makes a probe result
/// usable as the complete candidate set. One side of a disjunct is not, unless
/// both sides restrict the same property, in which case their union still is.
fn equality_candidates(
    predicate: &PhysicalExpression,
    binding: &Symbol,
) -> Vec<PropertyEqualityCandidate> {
    let mut candidates = Vec::new();
    collect_equality_candidates(predicate, binding, &mut candidates);
    candidates.dedup();
    candidates
}

fn collect_equality_candidates(
    predicate: &PhysicalExpression,
    binding: &Symbol,
    candidates: &mut Vec<PropertyEqualityCandidate>,
) {
    if let PhysicalExpression::Binary {
        left,
        operator: PhysicalBinaryOperator::And,
        right,
    } = predicate
    {
        collect_equality_candidates(left, binding, candidates);
        collect_equality_candidates(right, binding, candidates);
        return;
    }
    if let Some(candidate) = equality_candidate(predicate, binding) {
        candidates.push(candidate);
    }
}

fn equality_candidate(
    predicate: &PhysicalExpression,
    binding: &Symbol,
) -> Option<PropertyEqualityCandidate> {
    match predicate {
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Equal,
            right,
        } => {
            let (property, value) = candidate_property_value(left, right, binding)
                .or_else(|| candidate_property_value(right, left, binding))?;
            Some(PropertyEqualityCandidate {
                property,
                values: vec![value],
            })
        }
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Or,
            right,
        } => {
            let left = equality_candidate(left, binding)?;
            let right = equality_candidate(right, binding)?;
            if left.property != right.property {
                return None;
            }
            let mut values = left.values;
            for value in right.values {
                if !values.contains(&value) {
                    values.push(value);
                }
            }
            Some(PropertyEqualityCandidate {
                property: left.property,
                values,
            })
        }
        PhysicalExpression::InList { expression, values } => {
            let property = candidate_property(expression, binding)?;
            // NULL is not a seekable key, and Cypher's three-valued IN already
            // resolves it through the predicate itself.
            let values = values
                .iter()
                .filter(|value| !matches!(value.value, ScalarValue::Null))
                .map(|value| value.value.clone())
                .collect::<Vec<_>>();
            (!values.is_empty()).then_some(PropertyEqualityCandidate { property, values })
        }
        _ => None,
    }
}

fn candidate_property_value(
    property: &PhysicalExpression,
    value: &PhysicalExpression,
    binding: &Symbol,
) -> Option<(String, ScalarValue)> {
    let property = candidate_property(property, binding)?;
    let PhysicalExpression::Value(value) = value else {
        return None;
    };
    if matches!(value.value, ScalarValue::Null) {
        return None;
    }
    Some((property, value.value.clone()))
}

fn candidate_property(expression: &PhysicalExpression, binding: &Symbol) -> Option<String> {
    let PhysicalExpression::Property {
        binding: candidate_binding,
        property,
    } = expression
    else {
        return None;
    };
    (candidate_binding == binding).then(|| property.to_string())
}

/// How many different properties the candidates restrict. One is the property
/// the planner already anchored on, so a probe can only confirm it; two or more
/// means there is a genuine choice the planner never costed.
fn distinct_properties(candidates: &[PropertyEqualityCandidate]) -> usize {
    candidates
        .iter()
        .map(|candidate| candidate.property.as_str())
        .collect::<BTreeSet<_>>()
        .len()
}

/// Whether a whole binding row passes an ordered scan's residual predicate.
///
/// The vertex walk builds a one-binding row and uses [`residual_accepts`]; a
/// relationship walk already holds the source, relationship and target, so it
/// passes the row it built.
fn residual_row_accepts(
    residual: Option<&PhysicalExpression>,
    row: &BindingRow,
) -> EngineResult<bool> {
    let Some(residual) = residual else {
        return Ok(true);
    };
    predicate_holds(
        evaluate_expression(residual, row)?,
        "ordered scan residual did not evaluate to a boolean",
    )
}

/// Evaluate a scan's residual against one vertex.
///
/// `None` accepts everything, which is the case where the index proved the
/// whole predicate and no Filter was removed.
fn residual_accepts(
    residual: Option<&PhysicalExpression>,
    binding: &Symbol,
    vertex: &VertexRecord,
) -> EngineResult<bool> {
    let Some(residual) = residual else {
        return Ok(true);
    };
    let row = BindingRow {
        vertices: BTreeMap::from([(binding.clone(), vertex.clone())]),
        relationships: BTreeMap::new(),
        nulls: BTreeSet::new(),
        values: BTreeMap::new(),
        vertex_lists: BTreeMap::new(),
        relationship_lists: BTreeMap::new(),
    };
    predicate_holds(
        evaluate_expression(residual, &row)?,
        "ordered scan residual did not evaluate to a boolean",
    )
}

/// How much wider a page grows when a residual rejected every row of the last
/// one, leaving no miss rate to scale by.
const ORDERED_PAGE_GROWTH: usize = 4;

/// The most a page may exceed the rows still wanted. A residual that matches
/// almost nothing would otherwise walk the whole index in one request, and the
/// walk is supposed to be bounded work.
const ORDERED_PAGE_CEILING: usize = 64;

/// How far past its target an ordered walk reads before asking whether an
/// equality in its residual could be seeked instead.
///
/// The planner cannot tell a selective equality from a broad one, so the choice
/// between walking the sorted index and seeking an equality is settled here,
/// where both populations can be measured. This is the price of asking: low
/// enough that a query the walk cannot serve gives up after bounded work,
/// high enough that a window a residual thins by an ordinary fraction reaches
/// its target first and never pays for the probe at all.
const ORDERED_PROBE_TRIGGER: usize = 1;

/// Largest probe seed answered by loading it whole instead of filtering the
/// ordered walk with it. Matches legacy's speculative probe slice.
const ORDERED_SEED_HYDRATE_MAX: usize = 256;

/// Whether the walk has read past the primary value that decides the answer's
/// edge, and so has seen every row a tie key could reorder into it.
///
/// Rows arrive in primary-key order, which puts the row at `target - 1` on the
/// boundary. Rows sharing its value may sort ahead of it under a tie key the
/// index does not store, so all of them are candidates; rows past it lose on
/// the primary key, which no tie key can undo. `position` is where the index
/// left off rather than where the survivors did -- the residual rejecting a row
/// says nothing about how far the scan reached.
fn tie_boundary_passed(
    rows: &[BindingRow],
    binding: &Symbol,
    property: &str,
    target: usize,
    position: Option<&OrderedScanPosition>,
) -> bool {
    let Some(position) = position else {
        return false;
    };
    let boundary = rows
        .get(target.saturating_sub(1))
        .and_then(|row| row.vertices.get(binding))
        .and_then(|vertex| vertex.properties.get(property))
        .filter(|value| !matches!(value, ScalarValue::Null));
    // Compared under the index's own order, so a boundary of any stored type
    // is decided the same way -- and `1` and `1.0`, which compare equal, stay
    // one tie rather than two.
    boundary.is_some_and(|boundary| compare_property_values(&position.value, boundary).is_ne())
}

async fn hydrate_rows(
    read: &mut dyn GraphRead,
    binding: &Symbol,
    labels: &[Symbol],
    ids: Vec<VertexId>,
    row_limit: Option<usize>,
    residual: Option<&PhysicalExpression>,
) -> EngineResult<Vec<BindingRow>> {
    let unique_ids = ids
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if row_limit == Some(0) {
        return Ok(Vec::new());
    }

    let batch_size = row_limit.unwrap_or(unique_ids.len()).max(1);
    let mut rows = Vec::new();
    for ids in unique_ids.chunks(batch_size) {
        read.checkpoint("cypher_hydrate_rows")?;
        let mut vertices = read.hydrate_vertices(ids).await?;
        vertices.sort_by_key(|vertex| vertex.id);
        vertices.dedup_by_key(|vertex| vertex.id);
        for vertex in vertices {
            read.checkpoint("cypher_hydrate_vertex")?;
            if !labels
                .iter()
                .all(|label| vertex.labels.contains(label.as_str()))
            {
                continue;
            }
            let row = BindingRow {
                vertices: BTreeMap::from([(binding.clone(), vertex)]),
                relationships: BTreeMap::new(),
                nulls: BTreeSet::new(),
                values: BTreeMap::new(),
                vertex_lists: BTreeMap::new(),
                relationship_lists: BTreeMap::new(),
            };
            // A residual is only present when the caller removed the Filter
            // that would otherwise have applied it, so rejecting here is the
            // only thing keeping the result correct.
            if let Some(residual) = residual {
                if !predicate_holds(
                    evaluate_expression(residual, &row)?,
                    "Filter predicate did not evaluate to a boolean",
                )? {
                    continue;
                }
            }
            rows.push(row);
            read.check_intermediate_rows("cypher_hydrate_intermediate_rows", rows.len())?;
            if row_limit.is_some_and(|limit| rows.len() >= limit) {
                return Ok(rows);
            }
        }
    }
    Ok(rows)
}

/// Expand `rows` over one hop. `residuals` are the predicates of the Filters
/// a window was pushed through (see the `Filter` arm), innermost first; each
/// row the hop builds must pass them all to count against `row_limit`.
async fn expand_rows(
    read: &mut dyn GraphRead,
    rows: Vec<BindingRow>,
    spec: ExpandSpec<'_>,
    row_limit: Option<usize>,
    residuals: &[&PhysicalExpression],
) -> EngineResult<Vec<BindingRow>> {
    if row_limit == Some(0) {
        return Ok(Vec::new());
    }
    // A hop touching a name an OPTIONAL bound to null yields nothing for that
    // row: it starts nowhere if `from` is null, and no vertex or relationship
    // equals null, so a null `to` or relationship name cannot be satisfied
    // either. Dropping the row here is what eliminates it, and it also keeps
    // the row away from `binding_vertex`, which cannot tell null from a name
    // no plan produced and reports the latter as a query error.
    let rows = rows
        .into_iter()
        .filter(|row| {
            !row.is_null(spec.from)
                && !row.is_null(spec.to)
                && !spec
                    .relationship
                    .is_some_and(|binding| row.is_null(binding))
        })
        .collect::<Vec<_>>();
    let input_vertex_ids = rows
        .iter()
        .map(|row| {
            read.checkpoint("cypher_expand_inputs")?;
            binding_vertex(row, spec.from).map(|vertex| vertex.id)
        })
        .collect::<EngineResult<BTreeSet<_>>>()?;
    if input_vertex_ids.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(limit) = row_limit {
        if let Some(expanded) = expand_rows_windowed(read, &rows, &spec, limit, residuals).await? {
            return Ok(expanded);
        }
    }

    let requested_types = spec
        .relationship_types
        .iter()
        .map(Symbol::as_str)
        .collect::<BTreeSet<_>>();
    let mut by_input = BTreeMap::<VertexId, Vec<(VertexId, RelationshipRecord)>>::new();
    let mut relationship_rows = 0_usize;
    // An undirected hop runs as both storage orientations and merges here,
    // inside pattern matching, rather than as two plan-level union arms. Union
    // arms complete independently, so ORDER BY, DISTINCT and LIMIT would each
    // apply per arm instead of once across the merged rows.
    let mut seen_per_input = BTreeMap::<VertexId, BTreeSet<RelationshipId>>::new();
    for storage_direction in spec.direction.storage_directions().iter().copied() {
        let request = ExpandRequest {
            input_vertex_ids: input_vertex_ids.iter().copied().collect(),
            direction: storage_direction,
            relationship_types: spec
                .relationship_types
                .iter()
                .map(ToString::to_string)
                .collect(),
        };
        let returned = read.expand_relationships(&request).await?;
        let relationships = canonical_relationships(read, returned)?;
        for relationship in relationships.into_values() {
            read.checkpoint("cypher_expand_relationships")?;
            if !requested_types.is_empty()
                && !requested_types.contains(relationship.relationship_type.as_str())
            {
                continue;
            }
            let (input, target) = match storage_direction {
                ExpandDirection::Outgoing => (relationship.source, relationship.target),
                ExpandDirection::Incoming => (relationship.target, relationship.source),
            };
            if !input_vertex_ids.contains(&input) {
                continue;
            }
            // A self-loop is returned by both orientations. Keying on the
            // relationship identity reports it once while still reporting both
            // edges of a reciprocal pair, which are distinct relationships.
            if !seen_per_input
                .entry(input)
                .or_default()
                .insert(relationship.id)
            {
                continue;
            }
            by_input
                .entry(input)
                .or_default()
                .push((target, relationship));
            relationship_rows = relationship_rows.saturating_add(1);
            read.check_intermediate_rows("cypher_expand_relationship_rows", relationship_rows)?;
        }
    }
    for relationships in by_input.values_mut() {
        relationships.sort_by_key(|(target, relationship)| (*target, relationship.id));
    }

    let target_ids = by_input
        .values()
        .flatten()
        .map(|(target, _)| *target)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut hydrated = read.hydrate_vertices(&target_ids).await?;
    hydrated.sort_by_key(|vertex| vertex.id);
    hydrated.dedup_by_key(|vertex| vertex.id);
    let targets = hydrated
        .into_iter()
        .map(|vertex| (vertex.id, vertex))
        .collect::<BTreeMap<_, _>>();

    let mut expanded = Vec::new();
    for row in rows {
        read.checkpoint("cypher_expand_output_rows")?;
        let Some(input) = row.vertices.get(spec.from).map(|vertex| vertex.id) else {
            continue;
        };
        for (target_id, relationship) in by_input.get(&input).into_iter().flatten() {
            read.checkpoint("cypher_expand_output_rows")?;
            let Some(target) = targets.get(target_id) else {
                continue;
            };
            let Some(result) = expanded_row(&row, &spec, target, relationship) else {
                continue;
            };
            if !residuals_hold(&result, residuals)? {
                continue;
            }
            expanded.push(result);
            read.check_intermediate_rows("cypher_expand_intermediate_rows", expanded.len())?;
            if row_limit.is_some_and(|limit| expanded.len() >= limit) {
                return Ok(expanded);
            }
        }
    }
    Ok(expanded)
}

/// `row` extended over `relationship` to `target`, or `None` when the hop's
/// target labels or an already-bound `to` or relationship name reject it.
fn expanded_row(
    row: &BindingRow,
    spec: &ExpandSpec<'_>,
    target: &VertexRecord,
    relationship: &RelationshipRecord,
) -> Option<BindingRow> {
    if !spec
        .target_labels
        .iter()
        .all(|label| target.labels.contains(label.as_str()))
    {
        return None;
    }
    let mut result = row.clone();
    if let Some(existing) = result.vertices.get(spec.to) {
        if existing != target {
            return None;
        }
    } else {
        result.vertices.insert(spec.to.clone(), target.clone());
    }
    if let Some(binding) = spec.relationship {
        if let Some(existing) = result.relationships.get(binding) {
            if existing != relationship {
                return None;
            }
        } else {
            result
                .relationships
                .insert(binding.clone(), relationship.clone());
        }
    }
    Some(result)
}

/// Whether `row` passes every predicate of a stack of Filters, innermost
/// first. Evaluation stops at the first rejection, exactly as the stack did:
/// an outer predicate never sees a row an inner one discarded, so one that
/// would fail on such a row still does not.
fn residuals_hold(row: &BindingRow, residuals: &[&PhysicalExpression]) -> EngineResult<bool> {
    for predicate in residuals {
        if !predicate_holds(
            evaluate_expression(predicate, row)?,
            "Filter predicate did not evaluate to a boolean",
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The neighbours of one input that share a target, and the storage
/// orientations and types that reach it. A target is the unit a windowed
/// expansion reads: every relationship to it is read together, because the
/// hop orders an input's rows by target and then by relationship identity.
struct NeighborGroup {
    target: VertexId,
    /// `(index into storage_directions, relationship type)`.
    via: Vec<(usize, String)>,
}

/// Fewest inputs a windowed expansion lists neighbours for at once, so a hop
/// over many inputs that each reach little does not pay a round trip per
/// input.
const WINDOWED_EXPAND_MIN_LIST_INPUTS: usize = 64;

/// [`expand_rows`] under a `LIMIT`: the same rows in the same order, reading
/// only the relationships and targets the window needs.
///
/// The eager hop reads every relationship of every input and hydrates every
/// target before emitting its first row, so `LIMIT 50` from a vertex with
/// ten thousand relationships paid for all ten thousand. Here neighbours are
/// listed first -- the cheap adjacency read -- and relationship records are
/// read target by target in output order: by input row, then by target, which
/// is the order the eager hop sorts into. Each round reads the next `batch`
/// targets, emits what it can, and stops once `limit` rows passed `residuals`.
/// `batch` starts at the window and doubles every round, so a predicate that
/// rejects most rows costs a logarithmic number of rounds, not one per row.
///
/// `None` when the backend cannot list neighbours on their own; the caller
/// then runs the eager hop.
async fn expand_rows_windowed(
    read: &mut dyn GraphRead,
    rows: &[BindingRow],
    spec: &ExpandSpec<'_>,
    limit: usize,
    residuals: &[&PhysicalExpression],
) -> EngineResult<Option<Vec<BindingRow>>> {
    let storage_directions = spec.direction.storage_directions();
    let requested_types = spec
        .relationship_types
        .iter()
        .map(Symbol::as_str)
        .collect::<BTreeSet<_>>();
    let row_inputs = rows
        .iter()
        .map(|row| binding_vertex(row, spec.from).map(|vertex| vertex.id))
        .collect::<EngineResult<Vec<_>>>()?;
    // Listed inputs, each with its targets in ascending order.
    let mut groups = BTreeMap::<VertexId, Vec<NeighborGroup>>::new();
    // Read groups, keyed by `(input, target)`, relationships by identity.
    let mut fetched = BTreeMap::<(VertexId, VertexId), Vec<RelationshipRecord>>::new();
    // A self-loop is returned by both orientations; see the eager hop.
    let mut seen = BTreeSet::<(VertexId, RelationshipId)>::new();
    let mut targets = BTreeMap::<VertexId, Option<VertexRecord>>::new();
    let mut relationship_rows = 0_usize;
    let mut output = Vec::new();
    let (mut row_index, mut group_index) = (0_usize, 0_usize);
    let mut batch = limit.max(1);
    while row_index < rows.len() {
        read.checkpoint("cypher_expand_window")?;
        // List the next inputs, in row order, when the cursor reaches one
        // that is not listed yet.
        if !groups.contains_key(&row_inputs[row_index]) {
            let chunk_size = batch.max(WINDOWED_EXPAND_MIN_LIST_INPUTS);
            let mut chunk = BTreeSet::new();
            for input in &row_inputs[row_index..] {
                if chunk.len() >= chunk_size {
                    break;
                }
                if !groups.contains_key(input) {
                    chunk.insert(*input);
                }
            }
            let mut listed =
                BTreeMap::<VertexId, BTreeMap<VertexId, BTreeSet<(usize, String)>>>::new();
            for (direction_index, direction) in storage_directions.iter().copied().enumerate() {
                let request = ExpandRequest {
                    input_vertex_ids: chunk.iter().copied().collect(),
                    direction,
                    relationship_types: spec
                        .relationship_types
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                };
                let Some(neighbors) = read.expand_neighbors(&request).await? else {
                    if groups.is_empty() && direction_index == 0 {
                        return Ok(None);
                    }
                    return Err(EngineError::InvalidStorageResponse(
                        "the backend stopped listing neighbours partway through an expansion"
                            .to_string(),
                    ));
                };
                for neighbor in neighbors {
                    read.checkpoint("cypher_expand_neighbors")?;
                    if !chunk.contains(&neighbor.input)
                        || (!requested_types.is_empty()
                            && !requested_types.contains(neighbor.relationship_type.as_str()))
                    {
                        continue;
                    }
                    listed
                        .entry(neighbor.input)
                        .or_default()
                        .entry(neighbor.neighbor)
                        .or_default()
                        .insert((direction_index, neighbor.relationship_type));
                }
            }
            for input in chunk {
                let input_groups = listed
                    .remove(&input)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(target, via)| NeighborGroup {
                        target,
                        via: via.into_iter().collect(),
                    })
                    .collect();
                groups.insert(input, input_groups);
            }
        }

        // The unread targets among the next `batch`, in output order. The
        // walk stops at an input not listed yet; the next round lists it.
        // `(input, target)` to the target's position among the input's groups.
        let mut planned = BTreeMap::<(VertexId, VertexId), usize>::new();
        let (mut walk_row, mut walk_group, mut walked) = (row_index, group_index, 0_usize);
        while walk_row < rows.len() && walked < batch {
            let input = row_inputs[walk_row];
            let Some(input_groups) = groups.get(&input) else {
                break;
            };
            let Some(group) = input_groups.get(walk_group) else {
                walk_row += 1;
                walk_group = 0;
                continue;
            };
            if !fetched.contains_key(&(input, group.target)) {
                planned.insert((input, group.target), walk_group);
            }
            walk_group += 1;
            walked += 1;
        }

        if !planned.is_empty() {
            let mut read_now = BTreeMap::<(VertexId, VertexId), Vec<RelationshipRecord>>::new();
            for (direction_index, direction) in storage_directions.iter().copied().enumerate() {
                let neighbors = planned
                    .iter()
                    .flat_map(|(&(input, target), &position)| {
                        groups[&input][position]
                            .via
                            .iter()
                            .filter(move |(index, _)| *index == direction_index)
                            .map(move |(_, relationship_type)| ExpandNeighbor {
                                input,
                                neighbor: target,
                                relationship_type: relationship_type.clone(),
                            })
                    })
                    .collect::<Vec<_>>();
                if neighbors.is_empty() {
                    continue;
                }
                let returned = read
                    .expand_relationships_between(direction, &neighbors)
                    .await?;
                for relationship in canonical_relationships(read, returned)?.into_values() {
                    read.checkpoint("cypher_expand_relationships")?;
                    if !requested_types.is_empty()
                        && !requested_types.contains(relationship.relationship_type.as_str())
                    {
                        continue;
                    }
                    let (input, target) = match direction {
                        ExpandDirection::Outgoing => (relationship.source, relationship.target),
                        ExpandDirection::Incoming => (relationship.target, relationship.source),
                    };
                    if !planned.contains_key(&(input, target))
                        || !seen.insert((input, relationship.id))
                    {
                        continue;
                    }
                    read_now
                        .entry((input, target))
                        .or_default()
                        .push(relationship);
                    relationship_rows = relationship_rows.saturating_add(1);
                    read.check_intermediate_rows(
                        "cypher_expand_relationship_rows",
                        relationship_rows,
                    )?;
                }
            }
            let mut unhydrated = BTreeSet::new();
            for key in planned.into_keys() {
                let mut relationships = read_now.remove(&key).unwrap_or_default();
                relationships.sort_by_key(|relationship| relationship.id);
                if !relationships.is_empty() && !targets.contains_key(&key.1) {
                    unhydrated.insert(key.1);
                }
                fetched.insert(key, relationships);
            }
            if !unhydrated.is_empty() {
                let unhydrated = unhydrated.into_iter().collect::<Vec<_>>();
                for vertex in read.hydrate_vertices(&unhydrated).await? {
                    targets.entry(vertex.id).or_insert(Some(vertex));
                }
                for id in unhydrated {
                    targets.entry(id).or_insert(None);
                }
            }
        }

        // Emit from the cursor until a target or an input is not read yet.
        while let Some(&input) = row_inputs.get(row_index) {
            let Some(input_groups) = groups.get(&input) else {
                break;
            };
            let Some(group) = input_groups.get(group_index) else {
                row_index += 1;
                group_index = 0;
                continue;
            };
            let Some(relationships) = fetched.get(&(input, group.target)) else {
                break;
            };
            if let Some(Some(target)) = targets.get(&group.target) {
                for relationship in relationships {
                    read.checkpoint("cypher_expand_output_rows")?;
                    let Some(result) = expanded_row(&rows[row_index], spec, target, relationship)
                    else {
                        continue;
                    };
                    if !residuals_hold(&result, residuals)? {
                        continue;
                    }
                    output.push(result);
                    read.check_intermediate_rows("cypher_expand_intermediate_rows", output.len())?;
                    if output.len() >= limit {
                        return Ok(Some(output));
                    }
                }
            }
            group_index += 1;
        }
        batch = batch.saturating_mul(2);
    }
    Ok(Some(output))
}

/// Fewest frontier states a windowed variable-length walk reads
/// relationships for at once; see [`variable_expand_rows`].
const WINDOWED_WALK_MIN_STATES: usize = 64;

/// Walk `min_hops..=max_hops` hops breadth first: every row at one depth is
/// emitted, frontier state by frontier state, before any row of the next.
///
/// Under a window the frontier is read in chunks of states, in frontier
/// order, each chunk's inputs expanded and their targets hydrated only when
/// the window is still open; the first chunk is the rows still missing (at
/// least [`WINDOWED_WALK_MIN_STATES`]) and each next one doubles. Rows, the
/// next frontier and its order are what one whole-frontier read produces,
/// because a state's candidates do not depend on which chunk it is in. A
/// window that fills inside a depth therefore never reads the rest of that
/// depth, nor any deeper one.
///
/// The unit is a whole input, not a target as in [`expand_rows_windowed`]:
/// a walk orders an input's candidates by relationship identity, storage
/// orientation first, and identity is what an adjacency listing cannot
/// supply -- it lists targets, and a structural relationship's identity is
/// assigned only when its record is read. Reading one input's records in
/// target order would reorder its paths. So a single high-degree vertex at
/// the head of the frontier is still read whole.
#[allow(clippy::too_many_arguments)]
async fn variable_expand_rows(
    read: &mut dyn GraphRead,
    rows: Vec<BindingRow>,
    from: &Symbol,
    path_binding: Option<&Symbol>,
    relationship_types: &[Symbol],
    direction: PatternDirection,
    to: &Symbol,
    target_labels: &[Symbol],
    min_hops: u8,
    max_hops: Option<u8>,
    row_limit: Option<usize>,
    residuals: &[&PhysicalExpression],
) -> EngineResult<Vec<BindingRow>> {
    if row_limit == Some(0) {
        return Ok(Vec::new());
    }
    // As in `expand_rows`: a walk touching a null name yields nothing.
    let mut frontier = rows
        .into_iter()
        .filter(|row| !row.is_null(from) && !row.is_null(to))
        .map(|row| {
            let current = binding_vertex(&row, from)?.id;
            Ok(TraversalState {
                row,
                current,
                history: TraversalHistory {
                    path: path_binding.map(|_| std::sync::Arc::new(Vec::new())),
                    ..TraversalHistory::default()
                },
            })
        })
        .collect::<EngineResult<Vec<_>>>()?;
    let mut output = Vec::new();

    if min_hops == 0 {
        for state in &frontier {
            let source = binding_vertex(&state.row, from)?.clone();
            if target_labels
                .iter()
                .all(|label| source.labels.contains(label.as_str()))
            {
                let mut row = state.row.clone();
                if row
                    .vertices
                    .get(to)
                    .is_none_or(|existing| existing == &source)
                {
                    row.vertices.insert(to.clone(), source);
                    if bind_path(&mut row, path_binding, state.history.records())
                        && residuals_hold(&row, residuals)?
                    {
                        output.push(row);
                        read.check_intermediate_rows(
                            "cypher_variable_expand_output_rows",
                            output.len(),
                        )?;
                        if row_limit.is_some_and(|limit| output.len() >= limit) {
                            return Ok(output);
                        }
                    }
                }
            }
        }
    }

    // An undirected walk asks storage for both orientations at every hop and
    // merges them inside the walk -- not two directed walks, because the two
    // orientations compound: `a-b-a` over a reciprocal pair is a path either
    // run alone would miss. Trail semantics key on relationship identity, so
    // a self-loop, which both orientations return, is one step, and each edge
    // of a reciprocal pair is its own -- what Neo4j answers.
    let storage_directions = direction.storage_directions();
    let requested_types = relationship_types
        .iter()
        .map(Symbol::as_str)
        .collect::<BTreeSet<_>>();

    let mut depth: u32 = 0;
    loop {
        depth += 1;
        if frontier.is_empty() || max_hops.is_some_and(|max_hops| depth > u32::from(max_hops)) {
            break;
        }
        let mut by_input = BTreeMap::<VertexId, Vec<(VertexId, RelationshipRecord)>>::new();
        let mut targets = BTreeMap::<VertexId, VertexRecord>::new();
        let mut candidate_rows = 0_usize;
        let mut next = Vec::new();
        // Without a window the whole frontier is one chunk: one expansion
        // per orientation and one hydration per depth, as always.
        let mut chunk_size = match row_limit {
            Some(limit) => limit
                .saturating_sub(output.len())
                .max(WINDOWED_WALK_MIN_STATES),
            None => frontier.len(),
        };
        let mut states = frontier.into_iter();
        loop {
            let chunk = states.by_ref().take(chunk_size).collect::<Vec<_>>();
            if chunk.is_empty() {
                break;
            }
            chunk_size = chunk_size.saturating_mul(2);
            let input_vertex_ids = chunk
                .iter()
                .map(|state| state.current)
                .filter(|input| !by_input.contains_key(input))
                .collect::<BTreeSet<_>>();
            if !input_vertex_ids.is_empty() {
                let mut seen_per_input = BTreeMap::<VertexId, BTreeSet<RelationshipId>>::new();
                for storage_direction in storage_directions.iter().copied() {
                    let request = ExpandRequest {
                        input_vertex_ids: input_vertex_ids.iter().copied().collect(),
                        direction: storage_direction,
                        relationship_types: relationship_types
                            .iter()
                            .map(ToString::to_string)
                            .collect(),
                    };
                    let returned = read.expand_relationships(&request).await?;
                    let relationships = canonical_relationships(read, returned)?;
                    for relationship in relationships.into_values() {
                        read.checkpoint("cypher_variable_expand_relationships")?;
                        if !requested_types.is_empty()
                            && !requested_types.contains(relationship.relationship_type.as_str())
                        {
                            continue;
                        }
                        let (input, target) = match storage_direction {
                            ExpandDirection::Outgoing => (relationship.source, relationship.target),
                            ExpandDirection::Incoming => (relationship.target, relationship.source),
                        };
                        if input_vertex_ids.contains(&input)
                            && seen_per_input
                                .entry(input)
                                .or_default()
                                .insert(relationship.id)
                        {
                            candidate_rows += 1;
                            read.check_intermediate_rows(
                                "cypher_variable_expand_candidates",
                                candidate_rows,
                            )?;
                            by_input
                                .entry(input)
                                .or_default()
                                .push((target, relationship));
                        }
                    }
                }
                // An input with no candidates is read all the same.
                for input in &input_vertex_ids {
                    by_input.entry(*input).or_default();
                }
                let target_ids = input_vertex_ids
                    .iter()
                    .flat_map(|input| by_input[input].iter().map(|(target, _)| *target))
                    .filter(|target| !targets.contains_key(target))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                for vertex in read.hydrate_vertices(&target_ids).await? {
                    targets.entry(vertex.id).or_insert(vertex);
                }
            }
            for mut state in chunk {
                let candidates = by_input
                    .get(&state.current)
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                for (index, (target_id, relationship)) in candidates.iter().enumerate() {
                    read.checkpoint("cypher_variable_expand_paths")?;
                    if state.history.used.contains(&relationship.id) {
                        continue;
                    }
                    let Some(target) = targets.get(target_id) else {
                        continue;
                    };
                    // The final child takes ownership of its parent's history.
                    // A chain has one child, so both collections grow in place
                    // rather than copying every earlier hop at every depth.
                    // Siblings get copy-on-write histories and cannot mutate
                    // each other's trail.
                    let history = if index + 1 == candidates.len() {
                        std::mem::take(&mut state.history)
                    } else {
                        state.history.clone()
                    }
                    .extend(relationship);
                    let next_state = TraversalState {
                        row: state.row.clone(),
                        current: *target_id,
                        history,
                    };
                    if depth >= u32::from(min_hops)
                        && target_labels
                            .iter()
                            .all(|label| target.labels.contains(label.as_str()))
                        && next_state
                            .row
                            .vertices
                            .get(to)
                            .is_none_or(|existing| existing == target)
                    {
                        let mut row = next_state.row.clone();
                        row.vertices.insert(to.clone(), target.clone());
                        if bind_path(&mut row, path_binding, next_state.history.records())
                            && residuals_hold(&row, residuals)?
                        {
                            output.push(row);
                            read.check_intermediate_rows(
                                "cypher_variable_expand_output_rows",
                                output.len(),
                            )?;
                            if row_limit.is_some_and(|limit| output.len() >= limit) {
                                return Ok(output);
                            }
                        }
                    }
                    next.push(next_state);
                    read.check_intermediate_rows("cypher_variable_expand_frontier", next.len())?;
                }
            }
        }
        frontier = next;
    }
    Ok(output)
}

/// Bind a variable-length relationship name to the path's relationships, in
/// walk order, as a list value. `size(rs)` is then the hop count.
fn bind_path(row: &mut BindingRow, binding: Option<&Symbol>, path: &[RelationshipRecord]) -> bool {
    if let Some(binding) = binding {
        let value = QueryValue::List(
            path.iter()
                .map(|record| QueryValue::RelationshipId(record.id))
                .collect(),
        );
        if row
            .values
            .get(binding)
            .is_some_and(|existing| existing != &value)
        {
            return false;
        }
        if row.is_null(binding) {
            return false;
        }
        row.values.insert(binding.clone(), value);
        row.relationship_lists
            .insert(binding.clone(), std::sync::Arc::new(path.to_vec()));
    }
    true
}

fn canonical_relationships(
    read: &dyn GraphRead,
    relationships: Vec<RelationshipRecord>,
) -> EngineResult<BTreeMap<RelationshipId, RelationshipRecord>> {
    let mut canonical = BTreeMap::new();
    for relationship in relationships {
        read.checkpoint("cypher_canonical_relationships")?;
        if let Some(previous) = canonical.insert(relationship.id, relationship.clone()) {
            if previous != relationship {
                return Err(EngineError::InvalidStorageResponse(format!(
                    "relationship {} was returned with conflicting records",
                    relationship.id
                )));
            }
        }
    }
    Ok(canonical)
}

fn merge_compatible_rows(left: &BindingRow, right: &BindingRow) -> Option<BindingRow> {
    if left.vertices.iter().any(|(binding, vertex)| {
        right
            .vertices
            .get(binding)
            .is_some_and(|other| other != vertex)
    }) || left.relationships.iter().any(|(binding, relationship)| {
        right
            .relationships
            .get(binding)
            .is_some_and(|other| other != relationship)
    }) || left
        .vertices
        .keys()
        .any(|binding| right.relationships.contains_key(binding))
        || left
            .relationships
            .keys()
            .any(|binding| right.vertices.contains_key(binding))
        // Null equals nothing, itself included, so a side that bound a name
        // the other side calls null cannot be reconciled with it.
        || left
            .nulls
            .iter()
            .any(|binding| right.vertices.contains_key(binding)
                || right.relationships.contains_key(binding))
        || right
            .nulls
            .iter()
            .any(|binding| left.vertices.contains_key(binding)
                || left.relationships.contains_key(binding))
    {
        return None;
    }
    if left.values.iter().any(|(name, value)| {
        right
            .values
            .get(name)
            .is_some_and(|other| AggregateDistinctKey(value) != AggregateDistinctKey(other))
    }) {
        return None;
    }
    let mut merged = left.clone();
    merged.vertices.extend(right.vertices.clone());
    merged.relationships.extend(right.relationships.clone());
    merged.nulls.extend(right.nulls.iter().cloned());
    merged.values.extend(right.values.clone());
    merged.vertex_lists.extend(right.vertex_lists.clone());
    merged
        .relationship_lists
        .extend(right.relationship_lists.clone());
    Some(merged)
}

/// The property a relationship's `.id` reads: the identity the client wrote.
const RELATIONSHIP_ID_PROPERTY: &str = "id";

/// The relationship record `binding` names in `row`, wherever it came from: a
/// pattern or UNWIND bound it, or a computed projection (`coalesce(r, ...)`,
/// CASE, `min(r)`) carried it as a value whose record the row retained.
fn bound_relationship<'a>(row: &'a BindingRow, binding: &Symbol) -> Option<&'a RelationshipRecord> {
    if let Some(relationship) = row.relationships.get(binding) {
        return Some(relationship);
    }
    let Some(QueryValue::RelationshipId(id)) = row.values.get(binding) else {
        return None;
    };
    row.relationship_lists
        .get(binding)
        .into_iter()
        .flat_map(|records| records.iter())
        .chain(row.relationships.values())
        .chain(
            row.relationship_lists
                .values()
                .flat_map(|records| records.iter()),
        )
        .find(|record| record.id == *id)
}

fn evaluate_expression(
    expression: &PhysicalExpression,
    row: &BindingRow,
) -> EngineResult<EvaluatedValue> {
    Ok(match expression {
        PhysicalExpression::Binding(binding) => {
            if let Some(vertex) = row.vertices.get(binding) {
                EvaluatedValue::Vertex(vertex.id)
            } else if let Some(relationship) = row.relationships.get(binding) {
                EvaluatedValue::Relationship(relationship.id)
            } else if let Some(value) = row.values.get(binding) {
                evaluated_from_query_value(value)
            } else {
                EvaluatedValue::Null
            }
        }
        // A vertex's identity is the client's `id`. A relationship's is an
        // internal counter, and the client's `id` is its stored property --
        // however the relationship reached this name.
        PhysicalExpression::Identity(binding) => {
            if let Some(vertex) = row.vertices.get(binding) {
                EvaluatedValue::Vertex(vertex.id)
            } else if let Some(relationship) = bound_relationship(row, binding) {
                match relationship.properties.get(RELATIONSHIP_ID_PROPERTY) {
                    None | Some(ScalarValue::Null) => EvaluatedValue::Null,
                    Some(value) => EvaluatedValue::Scalar(value.clone()),
                }
            } else {
                EvaluatedValue::Null
            }
        }
        PhysicalExpression::Property { binding, property } => {
            let properties = if let Some(vertex) = row.vertices.get(binding) {
                &vertex.properties
            } else if let Some(relationship) = bound_relationship(row, binding) {
                &relationship.properties
            } else {
                // Includes the case of a name bound to a scalar: a scalar has
                // no properties, and Cypher reads a missing property as null.
                return Ok(EvaluatedValue::Null);
            };
            match properties.get(property.as_str()) {
                None | Some(ScalarValue::Null) => EvaluatedValue::Null,
                Some(value) => EvaluatedValue::Scalar(value.clone()),
            }
        }
        PhysicalExpression::Value(value) => match &value.value {
            ScalarValue::Null => EvaluatedValue::Null,
            value => EvaluatedValue::Scalar(value.clone()),
        },
        PhysicalExpression::Aggregate { .. } => {
            return Err(unsupported(
                QueryFailureReason::Return,
                "aggregate expressions must be evaluated by the projection stage",
            ));
        }
        PhysicalExpression::Unary {
            operator,
            expression,
        } => evaluate_unary(*operator, evaluate_expression(expression, row)?)?,
        PhysicalExpression::Binary {
            left,
            operator,
            right,
        } => evaluate_binary(
            *operator,
            evaluate_expression(left, row)?,
            evaluate_expression(right, row)?,
        )?,
        PhysicalExpression::InList { expression, values } => {
            evaluate_in_list(evaluate_expression(expression, row)?, values)
        }
        PhysicalExpression::Function {
            function,
            arguments,
        } => {
            let arguments = arguments
                .iter()
                .map(|argument| evaluate_expression(argument, row))
                .collect::<EngineResult<Vec<_>>>()?;
            evaluate_function(*function, arguments)?
        }
        PhysicalExpression::List(items) => EvaluatedValue::List(
            items
                .iter()
                .map(|item| evaluate_expression(item, row))
                .collect::<EngineResult<Vec<_>>>()?,
        ),
        PhysicalExpression::Case { .. } => {
            evaluate_case(expression, &mut |part| evaluate_expression(part, row))?
        }
    })
}

/// Every sub-expression of a CASE, in written order.
fn case_parts(expression: &PhysicalExpression) -> Vec<&PhysicalExpression> {
    let PhysicalExpression::Case {
        operand,
        branches,
        default,
    } = expression
    else {
        return Vec::new();
    };
    let mut parts: Vec<&PhysicalExpression> = operand.as_deref().into_iter().collect();
    for (when, then) in branches {
        parts.push(when);
        parts.push(then);
    }
    parts.extend(default.as_deref());
    parts
}

/// The value of a CASE, evaluating only what the chosen branch needs: the
/// operand, the `WHEN`s up to the first that holds, and its `THEN`. A simple
/// CASE compares with `=`, so a null operand matches no `WHEN`; a searched
/// CASE takes a `WHEN` only when it is true, not when it is null. With no
/// branch taken it is the `ELSE`, or null.
fn evaluate_case(
    expression: &PhysicalExpression,
    evaluate: &mut dyn FnMut(&PhysicalExpression) -> EngineResult<EvaluatedValue>,
) -> EngineResult<EvaluatedValue> {
    let PhysicalExpression::Case {
        operand,
        branches,
        default,
    } = expression
    else {
        unreachable!("evaluate_case is called on CASE only");
    };
    let operand = operand.as_deref().map(&mut *evaluate).transpose()?;
    for (when, then) in branches {
        let when = evaluate(when)?;
        let taken = match &operand {
            Some(operand) => evaluate_equal(operand.clone(), when) == Some(true),
            None => as_truth(when)? == Some(true),
        };
        if taken {
            return evaluate(then);
        }
    }
    default
        .as_deref()
        .map_or(Ok(EvaluatedValue::Null), evaluate)
}

/// Apply a scalar function to its already evaluated arguments.
///
/// Arguments are evaluated eagerly, before the function sees them. Cypher does
/// not short-circuit `coalesce` either: an argument that fails fails the call
/// even when an earlier one was not null.
fn evaluate_function(
    function: ScalarFunction,
    arguments: Vec<EvaluatedValue>,
) -> EngineResult<EvaluatedValue> {
    if function == ScalarFunction::RelationshipsUnique {
        let mut seen = BTreeSet::new();
        for argument in arguments {
            let relationships = match argument {
                EvaluatedValue::Relationship(id) => vec![EvaluatedValue::Relationship(id)],
                EvaluatedValue::List(values) => values,
                value if is_null(&value) => return Ok(EvaluatedValue::Truth(None)),
                _ => {
                    return Err(unsupported(
                        QueryFailureReason::Pattern,
                        "relationship uniqueness requires relationships",
                    ))
                }
            };
            for relationship in relationships {
                let EvaluatedValue::Relationship(id) = relationship else {
                    return Err(unsupported(
                        QueryFailureReason::Pattern,
                        "relationship trail contains a non-relationship",
                    ));
                };
                if !seen.insert(id) {
                    return Ok(EvaluatedValue::Truth(Some(false)));
                }
            }
        }
        return Ok(EvaluatedValue::Truth(Some(true)));
    }
    if function == ScalarFunction::Coalesce {
        return Ok(arguments
            .into_iter()
            .find(|argument| !is_null(argument))
            .unwrap_or(EvaluatedValue::Null));
    }
    // Every other function takes one argument, which lowering has checked,
    // and maps null to null.
    let argument = arguments.into_iter().next().ok_or_else(|| {
        unsupported(
            QueryFailureReason::Evaluation,
            format!("{} needs an argument", function.name()),
        )
    })?;
    if is_null(&argument) {
        return Ok(EvaluatedValue::Null);
    }
    if let (ScalarFunction::Size, EvaluatedValue::List(items)) = (function, &argument) {
        return Ok(EvaluatedValue::Scalar(ScalarValue::Integer(
            items.len() as i128
        )));
    }
    let string_argument = |argument: EvaluatedValue| match argument {
        EvaluatedValue::Scalar(ScalarValue::String(value)) => Ok(value),
        _ => Err(unsupported(
            QueryFailureReason::Evaluation,
            format!("{} requires a string argument", function.name()),
        )),
    };
    let string = |value: String| EvaluatedValue::Scalar(ScalarValue::String(value.into()));
    match function {
        ScalarFunction::Coalesce | ScalarFunction::RelationshipsUnique => {
            unreachable!("answered above")
        }
        ScalarFunction::ToUpper => Ok(string(string_argument(argument)?.to_uppercase())),
        ScalarFunction::ToLower => Ok(string(string_argument(argument)?.to_lowercase())),
        // Characters, not bytes and not UTF-16 units: Neo4j counts `'a😀'` as 2.
        ScalarFunction::Size => Ok(EvaluatedValue::Scalar(ScalarValue::Integer(
            string_argument(argument)?.chars().count() as i128,
        ))),
        ScalarFunction::ToString => Ok(match argument {
            EvaluatedValue::Scalar(ScalarValue::String(value)) => {
                EvaluatedValue::Scalar(ScalarValue::String(value))
            }
            EvaluatedValue::Scalar(ScalarValue::Integer(value)) => string(value.to_string()),
            EvaluatedValue::Scalar(ScalarValue::Boolean(value))
            | EvaluatedValue::Truth(Some(value)) => string(value.to_string()),
            EvaluatedValue::Scalar(ScalarValue::Float(spelling)) => {
                string(cypher_float_string(parse_float(&spelling)?))
            }
            _ => {
                return Err(unsupported(
                    QueryFailureReason::Evaluation,
                    "toString requires a string, number or boolean",
                ))
            }
        }),
        ScalarFunction::ToInteger => match argument {
            EvaluatedValue::Scalar(ScalarValue::Integer(value)) => {
                Ok(EvaluatedValue::Scalar(ScalarValue::Integer(value)))
            }
            EvaluatedValue::Scalar(ScalarValue::Boolean(value))
            | EvaluatedValue::Truth(Some(value)) => Ok(EvaluatedValue::Scalar(
                ScalarValue::Integer(i128::from(value)),
            )),
            EvaluatedValue::Scalar(ScalarValue::Float(spelling)) => {
                truncate_float(parse_float(&spelling)?)
            }
            // A string that spells no number is null rather than an error:
            // `toInteger('x')` is how a caller asks whether it is one.
            EvaluatedValue::Scalar(ScalarValue::String(value)) => {
                let value = value.trim();
                if let Ok(integer) = value.parse::<i64>() {
                    return Ok(EvaluatedValue::Scalar(ScalarValue::Integer(i128::from(
                        integer,
                    ))));
                }
                match value.parse::<f64>() {
                    Ok(float) if float.is_finite() => truncate_float(float),
                    _ => Ok(EvaluatedValue::Null),
                }
            }
            _ => Err(unsupported(
                QueryFailureReason::Evaluation,
                "toInteger requires a string, number or boolean",
            )),
        },
    }
}

fn parse_float(spelling: &str) -> EngineResult<f64> {
    spelling
        .parse::<f64>()
        .map_err(|_| unsupported(QueryFailureReason::Evaluation, "invalid float value"))
}

/// A float toward zero as a 64-bit integer, the range Cypher integers have.
fn truncate_float(value: f64) -> EngineResult<EvaluatedValue> {
    const I64_EXCLUSIVE_UPPER: f64 = 9_223_372_036_854_775_808.0;
    let truncated = value.trunc();
    if !truncated.is_finite()
        || truncated < -I64_EXCLUSIVE_UPPER
        || truncated >= I64_EXCLUSIVE_UPPER
    {
        return Err(unsupported(
            QueryFailureReason::Evaluation,
            "float is out of range for an integer",
        ));
    }
    Ok(EvaluatedValue::Scalar(ScalarValue::Integer(
        truncated as i128,
    )))
}

/// A float spelled the way Cypher spells one, which is Java's
/// `Double.toString`: plain decimal with at least one fractional digit for
/// magnitudes in `[1e-3, 1e7)`, otherwise `d.dddE±n`, both using the shortest
/// digits that round-trip. So `1.0`, `0.1` and `-0.0`, but `1.0E8` and
/// `1.0E21` where Rust would print `100000000` and `1e21`.
fn cypher_float_string(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    let magnitude = value.abs();
    if magnitude == 0.0 || (1e-3..1e7).contains(&magnitude) {
        // Rust's `Debug` is shortest round-trip and keeps a `.0`, and inside
        // this range it never switches to an exponent.
        return format!("{value:?}");
    }
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("`{:e}` always writes an exponent");
    if mantissa.contains('.') {
        format!("{mantissa}E{exponent}")
    } else {
        format!("{mantissa}.0E{exponent}")
    }
}

/// Cypher membership, which is three-valued.
///
/// An empty list is `false` even for a null probe: there is nothing it could
/// match. Otherwise a null probe is unknown, a hit is true, and a miss is
/// unknown when the list contains a null, because that null might have been
/// the match.
fn evaluate_in_list(probe: EvaluatedValue, values: &[BoundValue]) -> EvaluatedValue {
    if values.is_empty() {
        return EvaluatedValue::Truth(Some(false));
    }
    let EvaluatedValue::Scalar(probe) = probe else {
        return EvaluatedValue::Truth(None);
    };
    let mut saw_null = false;
    for candidate in values {
        match &candidate.value {
            ScalarValue::Null => saw_null = true,
            value if scalar_values_equal(&probe, value) => {
                return EvaluatedValue::Truth(Some(true))
            }
            _ => {}
        }
    }
    EvaluatedValue::Truth(if saw_null { None } else { Some(false) })
}

fn evaluate_unary(
    operator: PhysicalUnaryOperator,
    value: EvaluatedValue,
) -> EngineResult<EvaluatedValue> {
    match (operator, value) {
        (PhysicalUnaryOperator::IsNull, value) => Ok(EvaluatedValue::Truth(Some(is_null(&value)))),
        (PhysicalUnaryOperator::IsNotNull, value) => {
            Ok(EvaluatedValue::Truth(Some(!is_null(&value))))
        }
        (PhysicalUnaryOperator::Not, value) => {
            Ok(EvaluatedValue::Truth(as_truth(value)?.map(|value| !value)))
        }
        (PhysicalUnaryOperator::Plus, EvaluatedValue::Scalar(ScalarValue::Integer(value))) => {
            Ok(EvaluatedValue::Scalar(ScalarValue::Integer(value)))
        }
        (PhysicalUnaryOperator::Minus, EvaluatedValue::Scalar(ScalarValue::Integer(value))) => {
            Ok(EvaluatedValue::Scalar(ScalarValue::Integer(
                value.checked_neg().ok_or_else(|| {
                    unsupported(
                        QueryFailureReason::Evaluation,
                        "integer unary minus overflowed",
                    )
                })?,
            )))
        }
        (_, EvaluatedValue::Null) => Ok(EvaluatedValue::Null),
        _ => Err(unsupported(
            QueryFailureReason::Evaluation,
            "unary plus/minus currently requires an integer",
        )),
    }
}

fn evaluate_binary(
    operator: PhysicalBinaryOperator,
    left: EvaluatedValue,
    right: EvaluatedValue,
) -> EngineResult<EvaluatedValue> {
    Ok(match operator {
        PhysicalBinaryOperator::Equal => EvaluatedValue::Truth(evaluate_equal(left, right)),
        PhysicalBinaryOperator::NotEqual => {
            EvaluatedValue::Truth(evaluate_equal(left, right).map(|value| !value))
        }
        PhysicalBinaryOperator::LessThan => {
            EvaluatedValue::Truth(evaluate_ordering(left, right).map(Ordering::is_lt))
        }
        PhysicalBinaryOperator::LessThanOrEqual => {
            EvaluatedValue::Truth(evaluate_ordering(left, right).map(Ordering::is_le))
        }
        PhysicalBinaryOperator::GreaterThan => {
            EvaluatedValue::Truth(evaluate_ordering(left, right).map(Ordering::is_gt))
        }
        PhysicalBinaryOperator::GreaterThanOrEqual => {
            EvaluatedValue::Truth(evaluate_ordering(left, right).map(Ordering::is_ge))
        }
        PhysicalBinaryOperator::And => {
            EvaluatedValue::Truth(evaluate_and(as_truth(left)?, as_truth(right)?))
        }
        PhysicalBinaryOperator::Or => {
            EvaluatedValue::Truth(evaluate_or(as_truth(left)?, as_truth(right)?))
        }
        PhysicalBinaryOperator::Add
        | PhysicalBinaryOperator::Subtract
        | PhysicalBinaryOperator::Multiply
        | PhysicalBinaryOperator::Divide
        | PhysicalBinaryOperator::Modulo => evaluate_integer_arithmetic(operator, left, right)?,
        PhysicalBinaryOperator::StartsWith => evaluate_starts_with(left, right)?,
    })
}

fn query_value_to_evaluated(value: QueryValue) -> EngineResult<EvaluatedValue> {
    match value {
        QueryValue::Null => Ok(EvaluatedValue::Null),
        QueryValue::VertexId(id) => Ok(EvaluatedValue::Vertex(id)),
        QueryValue::RelationshipId(id) => Ok(EvaluatedValue::Relationship(id)),
        QueryValue::Count(value) => Ok(EvaluatedValue::Scalar(ScalarValue::Integer(i128::from(
            value,
        )))),
        QueryValue::Scalar(value) => Ok(EvaluatedValue::Scalar(value)),
        QueryValue::List(values) => Ok(EvaluatedValue::List(
            values
                .into_iter()
                .map(query_value_to_evaluated)
                .collect::<EngineResult<Vec<_>>>()?,
        )),
    }
}

fn binding_vertex<'a>(row: &'a BindingRow, binding: &Symbol) -> EngineResult<&'a VertexRecord> {
    row.vertices.get(binding).ok_or_else(|| {
        unsupported(
            QueryFailureReason::Other,
            format!("unbound vertex variable {binding}"),
        )
    })
}

fn evaluate_equal(left: EvaluatedValue, right: EvaluatedValue) -> Option<bool> {
    match (left, right) {
        (EvaluatedValue::Null, _) | (_, EvaluatedValue::Null) => None,
        (EvaluatedValue::Vertex(left), EvaluatedValue::Vertex(right)) => Some(left == right),
        (EvaluatedValue::Relationship(left), EvaluatedValue::Relationship(right)) => {
            Some(left == right)
        }
        (
            EvaluatedValue::Vertex(left) | EvaluatedValue::Relationship(left),
            EvaluatedValue::Scalar(ScalarValue::Integer(right)),
        )
        | (
            EvaluatedValue::Scalar(ScalarValue::Integer(right)),
            EvaluatedValue::Vertex(left) | EvaluatedValue::Relationship(left),
        ) => u64::try_from(right).ok().map(|right| left == right),
        (
            EvaluatedValue::Truth(Some(left)),
            EvaluatedValue::Scalar(ScalarValue::Boolean(right)),
        )
        | (
            EvaluatedValue::Scalar(ScalarValue::Boolean(right)),
            EvaluatedValue::Truth(Some(left)),
        ) => Some(left == right),
        (EvaluatedValue::Scalar(left), EvaluatedValue::Scalar(right)) => {
            Some(scalar_values_equal(&left, &right))
        }
        (EvaluatedValue::Truth(Some(left)), EvaluatedValue::Truth(Some(right))) => {
            Some(left == right)
        }
        (EvaluatedValue::Truth(None), _) | (_, EvaluatedValue::Truth(None)) => None,
        // Element-wise and three-valued: lists of different lengths differ,
        // a differing pair decides, and otherwise a null pair leaves it
        // unknown.
        (EvaluatedValue::List(left), EvaluatedValue::List(right)) => {
            if left.len() != right.len() {
                return Some(false);
            }
            let mut unknown = false;
            for (left, right) in left.into_iter().zip(right) {
                match evaluate_equal(left, right) {
                    Some(false) => return Some(false),
                    None => unknown = true,
                    Some(true) => {}
                }
            }
            (!unknown).then_some(true)
        }
        _ => Some(false),
    }
}

fn evaluate_ordering(left: EvaluatedValue, right: EvaluatedValue) -> Option<Ordering> {
    match (left, right) {
        (EvaluatedValue::Null, _) | (_, EvaluatedValue::Null) => None,
        (EvaluatedValue::Vertex(left), EvaluatedValue::Vertex(right))
        | (EvaluatedValue::Relationship(left), EvaluatedValue::Relationship(right)) => {
            Some(left.cmp(&right))
        }
        (
            EvaluatedValue::Vertex(left) | EvaluatedValue::Relationship(left),
            EvaluatedValue::Scalar(ScalarValue::Integer(right)),
        ) => i128::from(left).partial_cmp(&right),
        (
            EvaluatedValue::Scalar(ScalarValue::Integer(left)),
            EvaluatedValue::Vertex(right) | EvaluatedValue::Relationship(right),
        ) => left.partial_cmp(&i128::from(right)),
        (EvaluatedValue::Scalar(left), EvaluatedValue::Scalar(right)) => {
            Some(compare_property_values(&left, &right))
        }
        (EvaluatedValue::Truth(Some(left)), EvaluatedValue::Truth(Some(right))) => {
            Some(left.cmp(&right))
        }
        (EvaluatedValue::Truth(None), _) | (_, EvaluatedValue::Truth(None)) => None,
        (
            EvaluatedValue::Truth(Some(left)),
            EvaluatedValue::Scalar(ScalarValue::Boolean(right)),
        ) => Some(left.cmp(&right)),
        (
            EvaluatedValue::Scalar(ScalarValue::Boolean(left)),
            EvaluatedValue::Truth(Some(right)),
        ) => Some(left.cmp(&right)),
        (EvaluatedValue::List(left), EvaluatedValue::List(right)) => {
            let length = left.len().cmp(&right.len());
            for (left, right) in left.into_iter().zip(right) {
                match evaluate_ordering(left, right)? {
                    Ordering::Equal => {}
                    comparison => return Some(comparison),
                }
            }
            Some(length)
        }
        _ => None,
    }
}

fn evaluate_integer_arithmetic(
    operator: PhysicalBinaryOperator,
    left: EvaluatedValue,
    right: EvaluatedValue,
) -> EngineResult<EvaluatedValue> {
    let (left, right) = match (left, right) {
        (EvaluatedValue::Null, _) | (_, EvaluatedValue::Null) => {
            return Ok(EvaluatedValue::Null);
        }
        (
            EvaluatedValue::Scalar(ScalarValue::Integer(left)),
            EvaluatedValue::Scalar(ScalarValue::Integer(right)),
        ) => (left, right),
        _ => {
            return Err(unsupported(
                QueryFailureReason::Evaluation,
                "arithmetic operands must be integers",
            ))
        }
    };
    let value = match operator {
        PhysicalBinaryOperator::Add => left.checked_add(right),
        PhysicalBinaryOperator::Subtract => left.checked_sub(right),
        PhysicalBinaryOperator::Multiply => left.checked_mul(right),
        PhysicalBinaryOperator::Divide if right != 0 => left.checked_div(right),
        PhysicalBinaryOperator::Modulo if right != 0 => left.checked_rem(right),
        PhysicalBinaryOperator::Divide | PhysicalBinaryOperator::Modulo => {
            return Err(unsupported(
                QueryFailureReason::Evaluation,
                "arithmetic division by zero",
            ));
        }
        _ => {
            return Err(unsupported(
                QueryFailureReason::Evaluation,
                "operator is not arithmetic",
            ))
        }
    }
    .ok_or_else(|| {
        unsupported(
            QueryFailureReason::Evaluation,
            "integer arithmetic overflowed",
        )
    })?;
    Ok(EvaluatedValue::Scalar(ScalarValue::Integer(value)))
}

fn evaluate_starts_with(
    left: EvaluatedValue,
    right: EvaluatedValue,
) -> EngineResult<EvaluatedValue> {
    match (left, right) {
        (EvaluatedValue::Null, _) | (_, EvaluatedValue::Null) => Ok(EvaluatedValue::Truth(None)),
        (
            EvaluatedValue::Scalar(ScalarValue::String(value)),
            EvaluatedValue::Scalar(ScalarValue::String(prefix)),
        ) => Ok(EvaluatedValue::Truth(Some(
            value.starts_with(prefix.as_ref()),
        ))),
        _ => Err(unsupported(
            QueryFailureReason::Evaluation,
            "STARTS WITH operands must be strings",
        )),
    }
}

/// Whether a predicate's value keeps a row: true keeps it, false and unknown
/// drop it. A boolean value counts the same as a truth value, so
/// `WHERE true` and a CASE choosing `false` filter as a comparison does.
fn predicate_holds(value: EvaluatedValue, refusal: &'static str) -> EngineResult<bool> {
    match value {
        EvaluatedValue::Truth(Some(holds))
        | EvaluatedValue::Scalar(ScalarValue::Boolean(holds)) => Ok(holds),
        EvaluatedValue::Truth(None)
        | EvaluatedValue::Null
        | EvaluatedValue::Scalar(ScalarValue::Null) => Ok(false),
        _ => Err(unsupported(QueryFailureReason::Evaluation, refusal)),
    }
}

fn is_null(value: &EvaluatedValue) -> bool {
    matches!(
        value,
        EvaluatedValue::Null
            | EvaluatedValue::Scalar(ScalarValue::Null)
            | EvaluatedValue::Truth(None)
    )
}

fn as_truth(value: EvaluatedValue) -> EngineResult<Option<bool>> {
    match value {
        EvaluatedValue::Truth(value) => Ok(value),
        EvaluatedValue::Scalar(ScalarValue::Boolean(value)) => Ok(Some(value)),
        EvaluatedValue::Null => Ok(None),
        _ => Err(unsupported(
            QueryFailureReason::Evaluation,
            "OR operands must evaluate to booleans",
        )),
    }
}

fn evaluate_or(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

fn evaluate_and(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

/// A value a projection bound, read back as an evaluable value.
fn evaluated_from_query_value(value: &QueryValue) -> EvaluatedValue {
    match value {
        QueryValue::Null => EvaluatedValue::Null,
        QueryValue::VertexId(id) => EvaluatedValue::Vertex(*id),
        QueryValue::RelationshipId(id) => EvaluatedValue::Relationship(*id),
        QueryValue::Count(count) => {
            EvaluatedValue::Scalar(ScalarValue::Integer(i128::from(*count)))
        }
        QueryValue::Scalar(scalar) => EvaluatedValue::Scalar(scalar.clone()),
        QueryValue::List(values) => {
            EvaluatedValue::List(values.iter().map(evaluated_from_query_value).collect())
        }
    }
}

fn to_query_value(value: EvaluatedValue) -> QueryValue {
    match value {
        EvaluatedValue::Null | EvaluatedValue::Truth(None) => QueryValue::Null,
        EvaluatedValue::Vertex(id) => QueryValue::VertexId(id),
        EvaluatedValue::Relationship(id) => QueryValue::RelationshipId(id),
        EvaluatedValue::Scalar(value) => QueryValue::Scalar(value),
        EvaluatedValue::Truth(Some(value)) => QueryValue::Scalar(ScalarValue::Boolean(value)),
        EvaluatedValue::List(values) => {
            QueryValue::List(values.into_iter().map(to_query_value).collect())
        }
    }
}

fn bound_string(value: &BoundValue, role: &'static str) -> EngineResult<String> {
    match &value.value {
        ScalarValue::String(value) => Ok(value.to_string()),
        _ => Err(unsupported(
            QueryFailureReason::Evaluation,
            format!("ordered scan {role} must be a string"),
        )),
    }
}

fn seek_value(value: &BoundValue) -> EngineResult<&ScalarValue> {
    if matches!(value.value, ScalarValue::Null) {
        return Err(unsupported(
            QueryFailureReason::Evaluation,
            "NULL cannot be a property seek key",
        ));
    }
    Ok(&value.value)
}

fn vertex_id_value(value: &BoundValue) -> EngineResult<VertexId> {
    let ScalarValue::Integer(value) = value.value else {
        return Err(unsupported(
            QueryFailureReason::Evaluation,
            "vertex id must be an integer",
        ));
    };
    VertexId::try_from(value).map_err(|_| {
        unsupported(
            QueryFailureReason::Evaluation,
            "vertex id must be non-negative and fit u64",
        )
    })
}

fn window_count(value: &BoundValue, field: &'static str) -> EngineResult<usize> {
    let ScalarValue::Integer(value) = value.value else {
        return Err(unsupported(
            QueryFailureReason::OrderWindow,
            format!("{field} count is not an integer"),
        ));
    };
    usize::try_from(value).map_err(|_| {
        unsupported(
            QueryFailureReason::OrderWindow,
            format!("{field} count is out of range"),
        )
    })
}

fn unsupported(bucket: QueryFailureReason, reason: impl Into<String>) -> EngineError {
    EngineError::UnsupportedPlan(bucket, reason.into())
}
