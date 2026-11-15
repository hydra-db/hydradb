//! Statistics-driven access path selection.
//!
//! With a provider that ranks access candidates, the planner weighs every
//! equality and range on a vertex against each other and the label, and may
//! choose a bounded range seek. Whatever it chooses runs without the runtime
//! probe and declines past its read limit to the plan chosen without
//! statistics, which the probe may then narrow as it does today. Statistics
//! may change what is read, never what is returned: every execution test
//! below runs the same query planned without statistics and compares rows. A
//! provider that does not rank must plan exactly as before, byte for byte.

mod common;

use std::collections::BTreeMap;

use common::MemoryStorage;
use hydradb_cypher_engine::{
    estimate_operator_rows, AccessPathKind, CypherEngine, GraphLogicalPlan, GraphPhysicalPlan, ListParameterMap, ParameterMap,
    PreparedQuery, PropertyNumericRange, PropertyStatistics, QueryRow, QueryValue, ScalarValue, StatisticsProvider, Symbol,
    UnknownStatistics, VertexId, VertexRecord,
};
use hydradb_cypher_engine::{explain_physical_plan, PreparedVertexScan};

/// Rich counts from a provider that predates ranking: it answers every count
/// the planner asks for, but does not opt in to ranking access candidates.
struct LegacyRichStatistics;

impl StatisticsProvider for LegacyRichStatistics {
    fn vertex_count(&self, labels: &[Symbol]) -> Option<u64> {
        match labels {
            [label] if label.as_str() == "Source" => Some(50_000),
            [label] if label.as_str() == "Entity" => Some(40_000),
            _ => None,
        }
    }

    fn property_statistics(&self, _labels: &[Symbol], property: &Symbol) -> Option<PropertyStatistics> {
        let (non_null, distinct) = match property.as_str() {
            "tenant_id" | "sub_tenant_id" | "app_provider" => (500_000, 1),
            "entity_id" | "source_id" => (500_000, 500_000),
            "thread_id" => (500_000, 5_000),
            "created_at" => (500_000, 400_000),
            _ => return None,
        };
        Some(PropertyStatistics {
            non_null_count: Some(non_null),
            distinct_count: Some(distinct),
            ..PropertyStatistics::default()
        })
    }

    fn relationship_count(&self, _relationship_types: &[Symbol]) -> Option<u64> {
        Some(1_000)
    }
}

/// A provider that opts in to ranking, with whatever counts a test sets.
/// Property counts ignore labels, as the indexer-published ones do.
#[derive(Clone, Default)]
struct RankingStatistics {
    labels: BTreeMap<&'static str, u64>,
    /// `(total entries, distinct values)`.
    properties: BTreeMap<&'static str, (u64, u64)>,
    numeric: BTreeMap<&'static str, PropertyNumericRange>,
}

impl RankingStatistics {
    fn label(mut self, label: &'static str, count: u64) -> Self {
        self.labels.insert(label, count);
        self
    }

    fn property(mut self, property: &'static str, total: u64, distinct: u64) -> Self {
        self.properties.insert(property, (total, distinct));
        self
    }

    fn numeric(mut self, property: &'static str, min: f64, max: f64, entries: u64) -> Self {
        self.numeric.insert(property, PropertyNumericRange { min, max, entries });
        self
    }
}

impl StatisticsProvider for RankingStatistics {
    fn ranks_access_candidates(&self) -> bool {
        true
    }

    fn vertex_count(&self, labels: &[Symbol]) -> Option<u64> {
        match labels {
            [label] => self.labels.get(label.as_str()).copied(),
            _ => None,
        }
    }

    fn property_statistics(&self, _labels: &[Symbol], property: &Symbol) -> Option<PropertyStatistics> {
        let (total, distinct) = *self.properties.get(property.as_str())?;
        Some(PropertyStatistics {
            non_null_count: Some(total),
            distinct_count: Some(distinct),
            ..PropertyStatistics::default()
        })
    }

    /// Uniform: every value holds `total / distinct` entries.
    fn property_value_count(&self, _labels: &[Symbol], property: &Symbol, _value: &ScalarValue) -> Option<u64> {
        let (total, distinct) = *self.properties.get(property.as_str())?;
        Some(total.div_ceil(distinct.max(1)))
    }

    fn property_numeric_range(&self, _labels: &[Symbol], property: &Symbol) -> Option<PropertyNumericRange> {
        self.numeric.get(property.as_str()).copied()
    }
}

fn string(value: &str) -> ScalarValue {
    ScalarValue::String(value.into())
}

fn float(value: &str) -> ScalarValue {
    ScalarValue::Float(value.into())
}

fn corpus_parameters() -> (ParameterMap, ListParameterMap) {
    let parameters = ParameterMap::from([
        ("tenant_id".to_string(), string("t")),
        ("sub_tenant_id".to_string(), string("st")),
        ("app_provider".to_string(), string("slack")),
        ("thread_id".to_string(), string("thread-7")),
        ("since".to_string(), ScalarValue::Integer(499)),
        ("a".to_string(), string("thread-1")),
        ("b".to_string(), string("thread-2")),
        ("kind".to_string(), string("k")),
        // A negative float literal does not fold to a constant, so ranges on
        // one take it as a parameter.
        ("negative_float".to_string(), float("-1.5")),
        ("nan".to_string(), float("NaN")),
        // One past u64, so no index key can hold it.
        ("too_large".to_string(), ScalarValue::Integer(i128::from(u64::MAX) + 1)),
        ("fits".to_string(), ScalarValue::Integer(i128::from(u64::MAX))),
    ]);
    let lists = ListParameterMap::from([
        ("entity_ids".to_string(), vec![string("e1"), string("e2")]),
        ("ids".to_string(), vec![string("s1"), string("s2")]),
    ]);
    (parameters, lists)
}

/// Plan shapes whose flag-off planning must never move: the doc's examples
/// 2-4, label-only, IN, OR, ranges, stacked inline + WHERE filters, a
/// non-total outer filter, relationship filters, windows and joins.
const CORPUS: &[&str] = &[
    "MATCH (e:Entity {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id}) WHERE e.entity_id IN $entity_ids RETURN e.entity_id, e.name",
    "MATCH (s:Source {app_provider: $app_provider, tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id}) WHERE s.thread_id = $thread_id RETURN s.source_id",
    "MATCH (s:Source {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id}) WHERE s.created_at >= $since RETURN s.source_id",
    "MATCH (s:Source) RETURN s.source_id",
    "MATCH (s:Source) WHERE s.source_id IN $ids RETURN s",
    "MATCH (s:Source) WHERE s.thread_id = $a OR s.thread_id = $b RETURN s",
    "MATCH (s:Source) WHERE s.created_at >= 10 AND s.created_at < 20 RETURN s",
    "MATCH (s:Source {tenant_id: $tenant_id}) WHERE s.created_at > 10 / s.weight RETURN s",
    "MATCH (s:Source) WHERE s.created_at >= 10 RETURN s ORDER BY s.created_at LIMIT 3",
    "MATCH (a:Source)-[r:RELATES {kind: $kind}]->(b) RETURN b",
    "MATCH (a:Source)-[r:RELATES]->(b) WHERE r.weight >= 5 RETURN b",
    "MATCH (a:Source {tenant_id: $tenant_id})-[r:RELATES]->(b:Entity) WHERE a.created_at >= $since RETURN b",
    "MATCH (a:Entity {entity_id: 'e1'}) OPTIONAL MATCH (a)-[:RELATES]->(b:Source {tenant_id: $tenant_id}) WHERE b.created_at >= 3 RETURN a, b",
    "MATCH (s) WHERE s.created_at >= 10 RETURN s",
];

/// EXPLAIN output for the corpus with statistics ranking disabled. The
/// baseline keeps scalar-only property indexes away from predicates that can
/// also match native Date or List values.
const FLAG_OFF_GOLDEN: &str = include_str!("statistics_access_flag_off.golden");

fn prepare(query: &str, parameters: &ParameterMap, lists: &ListParameterMap, statistics: &dyn StatisticsProvider) -> PreparedQuery {
    CypherEngine::new(MemoryStorage::default())
        .lower_with_lists(query, parameters, lists)
        .expect("lowers")
        .plan_with_statistics(statistics)
        .expect("plans")
}

fn explain_corpus(query: &str, statistics: &dyn StatisticsProvider) -> String {
    let (parameters, lists) = corpus_parameters();
    prepare(query, &parameters, &lists, statistics).explain()
}

#[test]
fn flag_off_plans_preserve_native_safe_baseline() {
    let mut rendered = String::new();
    for query in CORPUS {
        rendered.push_str(&format!(
            "=== {query}\n--- unknown\n{}--- rich\n{}",
            explain_corpus(query, &UnknownStatistics),
            explain_corpus(query, &LegacyRichStatistics)
        ));
    }
    assert_eq!(rendered, FLAG_OFF_GOLDEN);
}

// ---- Ranking -------------------------------------------------------------

/// The doc's example scope: 500,000 vertices, tenant filters that match all of
/// them, one id property unique per vertex.
fn example_scope() -> RankingStatistics {
    RankingStatistics::default()
        .label("Source", 50_000)
        .label("Entity", 40_000)
        .property("tenant_id", 500_000, 1)
        .property("sub_tenant_id", 500_000, 1)
        .property("app_provider", 500_000, 2)
        .property("entity_id", 500_000, 500_000)
        .property("source_id", 500_000, 500_000)
        .property("thread_id", 500_000, 5_000)
        .property("created_at", 50_000, 50_000)
        .numeric("created_at", 0.0, 500.0, 50_000)
}

fn access(plan: &GraphPhysicalPlan) -> &GraphPhysicalPlan {
    match plan {
        GraphPhysicalPlan::Project { input, .. } | GraphPhysicalPlan::Filter { input, .. } => access(input),
        other => other,
    }
}

fn corpus_access(query: &str, statistics: &dyn StatisticsProvider) -> GraphPhysicalPlan {
    let (parameters, lists) = corpus_parameters();
    access(&prepare(query, &parameters, &lists, statistics).physical).clone()
}

fn vertex_scan(plan: &GraphPhysicalPlan) -> &PreparedVertexScan {
    match plan {
        GraphPhysicalPlan::VertexScan { scan } => scan,
        other => panic!("expected a vertex scan, got {other:?}"),
    }
}

fn seek_property(plan: &GraphPhysicalPlan, kind: &str) -> String {
    assert_eq!(vertex_scan(plan).access_kind(), kind, "{plan:?}");
    let explain = explain_physical_plan(plan);
    explain
        .lines()
        .next()
        .unwrap()
        .split("property=")
        .nth(1)
        .expect("seek property")
        .split(',')
        .next()
        .unwrap()
        .to_owned()
}

fn guarded_seek(plan: &GraphPhysicalPlan) -> String {
    seek_property(plan, "guarded-property")
}

#[test]
fn a_unique_id_beats_the_tenant_filters() {
    let plan = corpus_access(CORPUS[0], &example_scope());
    assert_eq!(guarded_seek(&plan), "entity_id");
    let explain = explain_physical_plan(&plan);
    assert!(explain.contains("values=[$entity_ids, $entity_ids]"), "{explain}");
    assert_eq!(vertex_scan(&plan).properties().estimated_rows, Some(2));
    assert!(explain.contains("estimated_rows=2, read_limit=256"), "{explain}");
    assert!(explain.contains("fallback:"), "{explain}");
    assert!(
        explain.contains("access=property-point, binding=e, labels=[Entity], property=tenant_id"),
        "{explain}"
    );
}

#[test]
fn a_narrow_equality_beats_three_broad_ones_even_past_the_probe_slice() {
    // 500,000 / 5,000 = 100 rows: more than one probe slice of 64, which is
    // exactly the case the probe used to lose.
    let plan = corpus_access(CORPUS[1], &example_scope());
    assert_eq!(guarded_seek(&plan), "thread_id");
    assert_eq!(vertex_scan(&plan).properties().estimated_rows, Some(100));
    assert!(explain_physical_plan(&plan).contains("read_limit=1000"));
}

#[test]
fn a_narrow_range_beats_the_tenant_and_the_label() {
    let (parameters, lists) = corpus_parameters();
    let prepared = prepare(CORPUS[2], &parameters, &lists, &example_scope());
    let plan = access(&prepared.physical);
    assert_eq!(vertex_scan(plan).access_kind(), "property-range");
    assert_eq!(vertex_scan(plan).properties().estimated_rows, Some(100));
    let explain = explain_physical_plan(plan);
    assert!(
        explain.contains("property=created_at, lower=inclusive $since, upper=none, estimated_rows=100, read_limit=1000"),
        "{explain}"
    );
    assert!(explain.contains("fallback:"), "{explain}");
    assert!(
        explain.contains("access=property-point, binding=s, labels=[Source], property=tenant_id"),
        "{explain}"
    );
    // The whole predicate still sits above the seek.
    let GraphPhysicalPlan::Project { input, .. } = &prepared.physical else {
        panic!("project root");
    };
    let GraphPhysicalPlan::Filter { predicate, .. } = input.as_ref() else {
        panic!("filter over the seek");
    };
    let rendered = prepared.explain();
    assert!(rendered.contains("(s.created_at >= $since)"), "{rendered}");
    assert!(matches!(predicate, hydradb_cypher_engine::PhysicalExpression::Binary { .. }));
}

#[test]
fn everything_broader_than_the_label_scans_the_label() {
    let statistics = example_scope().property("created_at", 500_000, 400_000).label("Source", 1_000);
    let plan = corpus_access(CORPUS[2], &statistics);
    assert_eq!(vertex_scan(&plan).access_kind(), "guarded-label");
    assert_eq!(vertex_scan(&plan).properties().estimated_rows, Some(1_000));
    let explain = explain_physical_plan(&plan);
    assert!(explain.contains("label=Source, estimated_rows=1000, read_limit=10000"), "{explain}");
}

#[test]
fn an_unknown_property_plans_as_without_statistics() {
    let statistics = RankingStatistics::default().label("Source", 50_000);
    for query in [CORPUS[1], CORPUS[2], CORPUS[6]] {
        assert_eq!(
            explain_corpus(query, &statistics),
            explain_corpus(query, &UnknownStatistics),
            "{query}"
        );
    }
}

#[test]
fn a_string_bound_is_not_a_range_candidate() {
    let statistics = example_scope();
    let query = "MATCH (s:Source {tenant_id: $tenant_id}) WHERE s.created_at >= 'a' RETURN s";
    let plan = corpus_access(query, &statistics);
    assert_ne!(vertex_scan(&plan).access_kind(), "property-range", "{plan:?}");
}

#[test]
fn two_bounds_on_one_property_are_one_candidate() {
    let plan = corpus_access(CORPUS[6], &example_scope());
    assert_eq!(vertex_scan(&plan).access_kind(), "property-range");
    let explain = explain_physical_plan(&plan);
    assert!(explain.contains("lower=inclusive 10, upper=exclusive 20"), "{explain}");
    assert_eq!(vertex_scan(&plan).properties().estimated_rows, Some(1_000));
}

#[test]
fn the_outer_where_is_visible_through_a_stacked_inline_filter() {
    let query = "MATCH (s:Source {tenant_id: $tenant_id}) WHERE s.thread_id = $thread_id RETURN s";
    let (parameters, lists) = corpus_parameters();
    // Lowering really does stack the two: the inline map inside, WHERE out.
    let logical = CypherEngine::new(MemoryStorage::default())
        .lower_with_lists(query, &parameters, &lists)
        .expect("lowers");
    let GraphLogicalPlan::Project { input, .. } = logical.logical.as_ref() else {
        panic!("project root");
    };
    assert!(
        matches!(input.as_ref(), GraphLogicalPlan::Filter { input, .. } if matches!(input.as_ref(), GraphLogicalPlan::Filter { .. })),
        "{input:?}"
    );
    // Without ranking, the inline map alone picks the access.
    let unranked = prepare(query, &parameters, &lists, &UnknownStatistics);
    assert_eq!(seek_property(access(&unranked.physical), "property-point"), "tenant_id");
    let ranked = prepare(query, &parameters, &lists, &example_scope());
    assert_eq!(guarded_seek(access(&ranked.physical)), "thread_id", "{}", ranked.explain());
    // Same Filter shape as before: the two layers still merge into one.
    assert_eq!(
        ranked.explain().matches("FilterExec").count(),
        unranked.explain().matches("FilterExec").count()
    );
}

#[test]
fn a_non_total_outer_layer_is_not_peeled() {
    let query = "MATCH (s:Source {tenant_id: $tenant_id}) WHERE s.thread_id = $thread_id AND s.created_at > 10 / s.weight RETURN s";
    let (parameters, lists) = corpus_parameters();
    let ranked = prepare(query, &parameters, &lists, &example_scope());
    let rendered = ranked.explain();
    // Only the inline tenant filter may choose the access; it is broader than
    // the label, so the label is scanned and thread_id is never sought.
    assert!(!rendered.contains("property=thread_id"), "{rendered}");
    assert!(
        rendered.contains("VertexScan(provider=slatedb, access=guarded-label, binding=s, labels=[Source], label=Source"),
        "{rendered}"
    );
    assert_eq!(rendered.matches("FilterExec").count(), 2, "{rendered}");
}

#[test]
fn ties_prefer_the_equality_then_the_earlier_conjunct() {
    let statistics = RankingStatistics::default()
        .label("Source", 50_000)
        .property("thread_id", 1_000, 10)
        .property("source_id", 1_000, 10)
        .property("created_at", 1_000, 1_000)
        .numeric("created_at", 0.0, 1_000.0, 1_000);
    // thread_id and source_id each estimate 100, as does the range.
    let query = "MATCH (s:Source) WHERE s.created_at >= 900 AND s.source_id = 'x' AND s.thread_id = 'y' RETURN s";
    let plan = corpus_access(query, &statistics);
    assert_eq!(guarded_seek(&plan), "source_id");
}

#[test]
fn the_range_seek_renders_and_redacts_statistics_in_its_shape() {
    let (parameters, lists) = corpus_parameters();
    let prepared = prepare(CORPUS[2], &parameters, &lists, &example_scope());
    let explain = prepared.explain();
    assert!(
        explain.contains(
            "VertexScan(provider=slatedb, access=property-range, binding=s, labels=[Source], property=created_at, lower=inclusive $since, upper=none, estimated_rows=100, read_limit=1000)"
        ),
        "{explain}"
    );
    let shape = prepared.plan_shape();
    assert!(shape.contains("estimated_rows=?, read_limit=?"), "{shape}");
    // A refresh that moves the estimate keeps the plan identity.
    let refreshed = prepare(
        CORPUS[2],
        &parameters,
        &lists,
        &example_scope().numeric("created_at", 0.0, 1_000.0, 50_000),
    );
    assert_ne!(prepared.explain(), refreshed.explain());
    assert_eq!(prepared.plan_hash(), refreshed.plan_hash());
    // Literal bounds never reach a shape.
    let literal = prepare(CORPUS[6], &parameters, &lists, &example_scope());
    assert!(!literal.plan_shape().contains("10"), "{}", literal.plan_shape());
}

#[test]
fn the_range_seek_estimates_its_own_rows() {
    let (parameters, lists) = corpus_parameters();
    let statistics = example_scope();
    let prepared = prepare(CORPUS[2], &parameters, &lists, &statistics);
    let estimates = estimate_operator_rows(&prepared.physical, &statistics);
    // Pre-order: Project, Filter, VertexScan. Its fallback is provider-internal.
    assert_eq!(estimates[2], Some(100));
}

/// The uniform-spread share applies to the numeric entries the range is
/// drawn from, not to every entry of the property: here nine in ten are
/// strings, which a two-sided numeric range never reads. A one-sided range
/// still adds them, because its walk reads on into them.
#[test]
fn the_range_share_applies_to_numeric_entries_only() {
    let statistics = RankingStatistics::default()
        .label("Source", 50_000)
        .property("created_at", 10_000, 10_000)
        .numeric("created_at", 0.0, 1_000.0, 1_000);
    let estimate = |query: &str| {
        let plan = corpus_access(query, &statistics);
        assert_eq!(vertex_scan(&plan).access_kind(), "property-range");
        vertex_scan(&plan).properties().estimated_rows.unwrap()
    };
    assert_eq!(
        estimate("MATCH (s:Source) WHERE s.created_at >= 0 AND s.created_at < 100 RETURN s"),
        100
    );
    assert_eq!(estimate("MATCH (s:Source) WHERE s.created_at >= 900 RETURN s"), 100 + 9_000);
}

// ---- Execution: rows never change ----------------------------------------

fn source(id: VertexId, created_at: Option<ScalarValue>) -> VertexRecord {
    let vertex = VertexRecord::new(id).with_label("Source").with_property("tenant_id", string("t"));
    match created_at {
        Some(value) => vertex.with_property("created_at", value),
        None => vertex,
    }
}

/// Integers, negative integers, floats, duplicates, values exactly on the
/// bounds, missing values, a string, a boolean and another label's vertex.
fn mixed_fixture() -> MemoryStorage {
    MemoryStorage::with_vertices([
        source(1, Some(ScalarValue::Integer(2))),
        source(2, Some(ScalarValue::Integer(3))),
        source(3, Some(ScalarValue::Integer(3))),
        source(4, Some(float("3.0"))),
        source(5, Some(ScalarValue::Integer(5))),
        source(6, Some(float("6.5"))),
        source(7, Some(ScalarValue::Integer(7))),
        source(8, Some(ScalarValue::Integer(7))),
        source(9, Some(ScalarValue::Integer(8))),
        source(10, None),
        source(11, Some(string("5"))),
        source(12, Some(ScalarValue::Boolean(true))),
        source(13, Some(ScalarValue::Integer(-2))),
        source(14, Some(float("-1.5"))),
        source(15, Some(float("2.5"))),
        VertexRecord::new(16)
            .with_label("Other")
            .with_property("tenant_id", string("t"))
            .with_property("created_at", ScalarValue::Integer(5)),
    ])
}

fn mixed_statistics() -> RankingStatistics {
    RankingStatistics::default()
        .label("Source", 50_000)
        .property("tenant_id", 500_000, 1)
        .property("created_at", 1_000, 900)
        .numeric("created_at", -100.0, 100.0, 1_000)
}

fn ids(rows: &[QueryRow]) -> Vec<u64> {
    rows.iter()
        .map(|row| match row.values.as_slice() {
            [QueryValue::VertexId(id)] => *id,
            other => panic!("unexpected row {other:?}"),
        })
        .collect()
}

async fn run(storage: &MemoryStorage, query: &str, statistics: &dyn StatisticsProvider) -> (PreparedQuery, Vec<u64>) {
    let (parameters, lists) = corpus_parameters();
    let prepared = prepare(query, &parameters, &lists, statistics);
    let result = CypherEngine::new(storage.clone())
        .execute_prepared(&prepared)
        .await
        .expect("executes");
    (prepared, ids(&result.rows))
}

/// Run `query` with and without statistics, require a range seek in the
/// first, and return the rows, which must agree.
async fn same_rows_as_filter_only(storage: &MemoryStorage, query: &str, statistics: &dyn StatisticsProvider) -> Vec<u64> {
    let (ranked, ranked_rows) = run(storage, query, statistics).await;
    assert!(ranked.explain().contains("access=property-range"), "{}", ranked.explain());
    let (unranked, unranked_rows) = run(storage, query, &UnknownStatistics).await;
    assert!(!unranked.explain().contains("access=property-range"));
    assert_eq!(ranked_rows, unranked_rows, "{query}");
    ranked_rows
}

#[tokio::test]
async fn range_seek_rows_match_the_filter_only_plan_with_nulls_duplicates_and_bounds() {
    let storage = mixed_fixture();
    let rows = same_rows_as_filter_only(
        &storage,
        "MATCH (s:Source {tenant_id: 't'}) WHERE s.created_at >= 3 AND s.created_at <= 7 RETURN s",
        &mixed_statistics(),
    )
    .await;
    // Both 3s, the float 3.0, both 7s: the duplicates and the exact bounds.
    // Never the missing value, the string '5', the boolean or label Other.
    assert_eq!(rows, [2, 3, 4, 5, 6, 7, 8]);
}

#[tokio::test]
async fn inclusive_and_exclusive_bounds_keep_the_right_edge_rows() {
    let storage = mixed_fixture();
    let statistics = mixed_statistics();
    let cases: [(&str, &[u64]); 4] = [
        ("s.created_at > 3 AND s.created_at < 7", &[5, 6]),
        ("s.created_at >= 3 AND s.created_at < 7", &[2, 3, 4, 5, 6]),
        ("s.created_at > 3 AND s.created_at <= 7", &[5, 6, 7, 8]),
        ("3 < s.created_at AND 7 >= s.created_at", &[5, 6, 7, 8]),
    ];
    for (predicate, expected) in cases {
        let query = format!("MATCH (s:Source) WHERE {predicate} RETURN s");
        assert_eq!(
            same_rows_as_filter_only(&storage, &query, &statistics).await,
            expected,
            "{predicate}"
        );
    }
}

#[tokio::test]
async fn two_bounds_give_one_bounded_read() {
    let storage = mixed_fixture();
    same_rows_as_filter_only(
        &storage,
        "MATCH (s:Source) WHERE s.created_at >= 3 AND s.created_at < 7 RETURN s",
        &mixed_statistics(),
    )
    .await;
    let requests = storage.ordered_scan_requests.lock().unwrap();
    assert_eq!(requests.len(), 1, "one page covers the range: {requests:?}");
    let request = &requests[0];
    assert_eq!(
        request.lower.as_ref().map(|bound| (&bound.value, bound.inclusive)),
        Some((&ScalarValue::Integer(3), true))
    );
    assert_eq!(
        request.upper.as_ref().map(|bound| (&bound.value, bound.inclusive)),
        Some((&ScalarValue::Integer(7), false))
    );
    assert!(request.max_index_entries.is_some());
}

#[tokio::test]
async fn a_range_spanning_integers_and_floats() {
    let storage = mixed_fixture();
    let statistics = mixed_statistics();
    let rows = same_rows_as_filter_only(
        &storage,
        "MATCH (s:Source) WHERE s.created_at >= -2 AND s.created_at < 3.0 RETURN s",
        &statistics,
    )
    .await;
    assert_eq!(rows, [1, 13, 14, 15]);
    // Open-ended: the walk runs on past the numbers into the strings above
    // them (or the booleans below), and whatever the Filter decides about
    // those, both plans decide it alike.
    let rows = same_rows_as_filter_only(&storage, "MATCH (s:Source) WHERE s.created_at > 6.5 RETURN s", &statistics).await;
    assert!(rows.starts_with(&[7, 8, 9]), "{rows:?}");
    let rows = same_rows_as_filter_only(
        &storage,
        "MATCH (s:Source) WHERE s.created_at <= $negative_float RETURN s",
        &statistics,
    )
    .await;
    assert!(rows.contains(&13) && rows.contains(&14), "{rows:?}");
}

#[tokio::test]
async fn missing_stale_and_future_statistics_change_the_plan_never_the_rows() {
    let storage = mixed_fixture();
    let query = "MATCH (s:Source {tenant_id: 't'}) WHERE s.created_at >= 3 AND s.created_at <= 7 RETURN s";
    let (_, expected) = run(&storage, query, &UnknownStatistics).await;
    let missing = RankingStatistics::default();
    // Counts from a much smaller graph, and a domain that misses every value.
    let stale = mixed_statistics().property("created_at", 2, 2).numeric("created_at", 50.0, 60.0, 2);
    // A later build in which the tenant became narrow and the range broad.
    let future = mixed_statistics()
        .property("tenant_id", 5, 5)
        .property("created_at", 1_000_000, 900);
    let mut plans = Vec::new();
    for statistics in [missing, stale, future] {
        let (prepared, rows) = run(&storage, query, &statistics).await;
        assert_eq!(rows, expected, "{}", prepared.explain());
        plans.push(prepared.explain());
    }
    assert!(plans[1].contains("access=property-range"), "{}", plans[1]);
    assert!(plans[2].contains("property=tenant_id"), "{}", plans[2]);
    assert!(!plans[2].contains("access=property-range"), "{}", plans[2]);
}

fn wide_fixture(sources_in_range: u64, others_in_range: u64) -> MemoryStorage {
    let sources = (1..=sources_in_range).map(|id| source(id, Some(ScalarValue::Integer((id % 50) as i128))));
    let others = (0..others_in_range).map(|offset| {
        VertexRecord::new(10_000 + offset)
            .with_label("Other")
            .with_property("created_at", ScalarValue::Integer((offset % 50) as i128))
    });
    let outside = (0..20).map(|offset| source(20_000 + offset, Some(ScalarValue::Integer(1_000 + offset as i128))));
    MemoryStorage::with_vertices(sources.chain(others).chain(outside))
}

/// Statistics that think the range holds one entry.
fn optimistic_statistics() -> RankingStatistics {
    RankingStatistics::default()
        .label("Source", 50_000)
        .property("tenant_id", 500_000, 1)
        .property("created_at", 1_000, 1_000)
        .numeric("created_at", 0.0, 1_000_000.0, 1_000)
}

#[tokio::test]
async fn a_range_far_wider_than_its_estimate_declines_to_the_same_rows() {
    let storage = wide_fixture(600, 0);
    let query = "MATCH (s:Source {tenant_id: 't'}) WHERE s.created_at >= 0 AND s.created_at < 100 RETURN s";
    let (prepared, rows) = run(&storage, query, &optimistic_statistics()).await;
    let explain = prepared.explain();
    assert!(explain.contains("estimated_rows=1, read_limit=256"), "{explain}");
    let (_, expected) = run(&storage, query, &UnknownStatistics).await;
    assert_eq!(rows, expected);
    assert_eq!(rows.len(), 600);
    let reports = storage.statistics_access.lock().unwrap();
    let [report] = reports.as_slice() else {
        panic!("one report: {reports:?}");
    };
    assert_eq!(report.kind, AccessPathKind::Range);
    assert_eq!(report.property.as_deref(), Some("created_at"));
    assert!(report.declined);
    assert_eq!(report.entries_read, 257, "stops one entry past the limit");
    assert_eq!(*storage.ordered_scan_entries.lock().unwrap(), 257);
}

/// A range seek's reads are bounded by its own limit, not charged to the
/// query's index-candidate cap. Under a cap configured below that limit the
/// seek must still stop within the cap, then decline to the same rows.
#[tokio::test]
async fn a_range_seek_never_reads_past_the_query_index_candidate_cap() {
    let storage = MemoryStorage {
        max_query_index_candidates: Some(100),
        ..wide_fixture(600, 0)
    };
    let query = "MATCH (s:Source {tenant_id: 't'}) WHERE s.created_at >= 0 AND s.created_at < 100 RETURN s";
    let (prepared, rows) = run(&storage, query, &optimistic_statistics()).await;
    assert!(prepared.explain().contains("read_limit=256"), "{}", prepared.explain());
    let (_, expected) = run(&storage, query, &UnknownStatistics).await;
    assert_eq!(rows, expected);
    let reports = storage.statistics_access.lock().unwrap();
    let [report] = reports.as_slice() else {
        panic!("one report: {reports:?}");
    };
    assert!(report.declined);
    assert_eq!(report.entries_read, 100, "stops at the cap, not at its own limit");
    assert_eq!(*storage.ordered_scan_entries.lock().unwrap(), 100);
}

/// A statistics-chosen access is an optional shortcut: reaching the query's
/// intermediate row cap proves only that it was a poor choice. A range wider
/// than its estimate must decline to the plan chosen without statistics --
/// here a seek on a tenant nothing belongs to -- rather than fail the query.
#[tokio::test]
async fn a_range_seek_declines_before_the_intermediate_row_cap() {
    let storage = MemoryStorage {
        max_intermediate_rows: Some(100),
        ..wide_fixture(600, 0)
    };
    let query = "MATCH (s:Source {tenant_id: 'x'}) WHERE s.created_at >= 0 AND s.created_at < 100 RETURN s";
    let (prepared, rows) = run(&storage, query, &optimistic_statistics()).await;
    assert!(prepared.explain().contains("access=property-range"), "{}", prepared.explain());
    let (_, expected) = run(&storage, query, &UnknownStatistics).await;
    assert_eq!(rows, expected);
    assert!(rows.is_empty());
    let reports = storage.statistics_access.lock().unwrap();
    let [report] = reports.as_slice() else {
        panic!("one report: {reports:?}");
    };
    assert!(report.declined);
    assert_eq!(report.entries_read, 100, "stops at the row cap, not at its own limit");
}

/// The guarded equality seek likewise declines under the intermediate row
/// cap rather than hydrating more rows than the query may hold.
#[tokio::test]
async fn a_guarded_seek_declines_before_the_intermediate_row_cap() {
    let storage = MemoryStorage {
        max_intermediate_rows: Some(100),
        ..MemoryStorage::with_vertices((1..=150).map(|id| source(id, Some(ScalarValue::Integer(3)))))
    };
    let query = "MATCH (s:Source {tenant_id: 'x'}) WHERE s.created_at = 3 RETURN s";
    let (prepared, rows) = run(&storage, query, &optimistic_statistics()).await;
    assert!(prepared.explain().contains("access=guarded-"), "{}", prepared.explain());
    assert!(prepared.explain().contains("property=created_at"), "{}", prepared.explain());
    let (_, expected) = run(&storage, query, &UnknownStatistics).await;
    assert_eq!(rows, expected);
    assert!(rows.is_empty());
    assert_eq!(*storage.bounded_seek_limits.lock().unwrap(), [99]);
    let reports = storage.statistics_access.lock().unwrap();
    assert!(reports.iter().all(|report| report.declined), "{reports:?}");
}

/// A backend without its own bounded walk gets the trait's default, which
/// declines at once rather than seeking every match before judging. The
/// profiling wrapper forwards no bounded seek, so it stands in for one.
#[tokio::test]
async fn a_backend_without_a_bounded_seek_declines_without_reading() {
    let inner = MemoryStorage::with_vertices((1..=150).map(|id| source(id, Some(ScalarValue::Integer(3)))));
    let query = "MATCH (s:Source {tenant_id: 't'}) WHERE s.created_at = 3 RETURN s";
    let (parameters, lists) = corpus_parameters();
    let prepared = prepare(query, &parameters, &lists, &optimistic_statistics());
    assert!(prepared.explain().contains("access=guarded-"), "{}", prepared.explain());
    let result = CypherEngine::new(common::profiling::ProfilingStorage::new(inner.clone()))
        .execute_prepared(&prepared)
        .await
        .expect("executes");
    let (_, expected) = run(&inner, query, &UnknownStatistics).await;
    assert_eq!(ids(&result.rows), expected);
    assert_eq!(expected.len(), 150);
    assert!(
        !inner.seek_values.lock().unwrap().contains(&ScalarValue::Integer(3)),
        "the statistics-chosen property is never seeked whole"
    );
    let reports = inner.statistics_access.lock().unwrap();
    let [report] = reports.as_slice() else {
        panic!("one report: {reports:?}");
    };
    assert!(report.declined);
    assert_eq!(report.entries_read, 0, "a decline that read nothing reports no reads");
}

/// Under an intermediate row cap of zero the range seek may keep no entry,
/// and even its first would fail the row check before the guard declined.
/// It declines before reading, and the fallback answers.
#[tokio::test]
async fn a_range_seek_under_a_zero_row_cap_declines_before_reading() {
    let storage = MemoryStorage {
        max_intermediate_rows: Some(0),
        ..wide_fixture(600, 0)
    };
    let query = "MATCH (s:Source {tenant_id: 'x'}) WHERE s.created_at >= 0 AND s.created_at < 100 RETURN s";
    let (prepared, rows) = run(&storage, query, &optimistic_statistics()).await;
    assert!(prepared.explain().contains("access=property-range"), "{}", prepared.explain());
    assert!(rows.is_empty());
    let reports = storage.statistics_access.lock().unwrap();
    let [report] = reports.as_slice() else {
        panic!("one report: {reports:?}");
    };
    assert!(report.declined);
    assert_eq!(report.entries_read, 0);
    assert_eq!(*storage.ordered_scan_entries.lock().unwrap(), 0);
}

#[tokio::test]
async fn a_range_inside_its_limit_answers_and_reports_what_it_read() {
    let storage = wide_fixture(100, 0);
    let query = "MATCH (s:Source {tenant_id: 't'}) WHERE s.created_at >= 0 AND s.created_at < 100 RETURN s";
    let (_, rows) = run(&storage, query, &optimistic_statistics()).await;
    let (_, expected) = run(&storage, query, &UnknownStatistics).await;
    assert_eq!(rows, expected);
    let reports = storage.statistics_access.lock().unwrap();
    assert_eq!(reports.len(), 1);
    assert!(!reports[0].declined);
    assert_eq!(reports[0].entries_read, 100);
}

#[tokio::test]
async fn entries_of_other_labels_count_toward_the_limit() {
    // Two Source vertices in range, 300 of another label in the same index.
    let storage = wide_fixture(2, 300);
    let query = "MATCH (s:Source {tenant_id: 't'}) WHERE s.created_at >= 0 AND s.created_at < 100 RETURN s";
    let (_, rows) = run(&storage, query, &optimistic_statistics()).await;
    let (_, expected) = run(&storage, query, &UnknownStatistics).await;
    assert_eq!(rows, expected);
    assert_eq!(rows, [1, 2]);
    let reports = storage.statistics_access.lock().unwrap();
    assert!(reports[0].declined, "{reports:?}");
}

#[tokio::test]
async fn a_correlated_range_seek_returns_the_same_rows() {
    let storage = mixed_fixture();
    // The OPTIONAL side runs once per outer row, seeded with it.
    let query = "MATCH (a:Other) OPTIONAL MATCH (s:Source) WHERE s.created_at >= 3 AND s.created_at <= 7 RETURN s";
    let (ranked, rows) = run(&storage, query, &mixed_statistics()).await;
    assert!(ranked.explain().contains("access=property-range"), "{}", ranked.explain());
    let (_, expected) = run(&storage, query, &UnknownStatistics).await;
    assert_eq!(rows, expected);
    assert_eq!(rows, [2, 3, 4, 5, 6, 7, 8]);
}

// ---- Step 4: the probe becomes a guard -------------------------------------

/// `count` Source vertices of tenant `t`, all on thread `hot`, a third of them
/// on provider `slack`, with `created_at` cycling through 0..50.
fn tenant_sources(count: u64) -> Vec<VertexRecord> {
    (1..=count)
        .map(|id| {
            let vertex = VertexRecord::new(id)
                .with_label("Source")
                .with_property("tenant_id", string("t"))
                .with_property("thread_id", string("hot"))
                .with_property("created_at", ScalarValue::Integer((id % 50) as i128));
            if id % 3 == 0 {
                vertex.with_property("app_provider", string("slack"))
            } else {
                vertex
            }
        })
        .collect()
}

/// A probe-capable backend over `vertices`, willing to call `selective`
/// narrow.
fn probing_storage(vertices: Vec<VertexRecord>, selective: &[&str]) -> MemoryStorage {
    MemoryStorage {
        selective_properties: selective.iter().map(ToString::to_string).collect(),
        ..MemoryStorage::with_vertices(vertices)
    }
}

/// Statistics that think thread `hot` holds `per_thread` vertices, the tenant
/// all of them, and the label 50,000.
fn thread_statistics(per_thread: u64) -> RankingStatistics {
    RankingStatistics::default()
        .label("Source", 50_000)
        .property("tenant_id", 500_000, 1)
        .property("thread_id", 500_000, 500_000 / per_thread.max(1))
}

const THREAD_QUERY: &str = "MATCH (s:Source {tenant_id: 't'}) WHERE s.thread_id = 'hot' RETURN s";

fn reports(storage: &MemoryStorage) -> Vec<hydradb_cypher_engine::StatisticsAccessReport> {
    storage.statistics_access.lock().unwrap().clone()
}

#[tokio::test]
async fn a_confident_equality_seeks_without_the_probe() {
    let storage = probing_storage(tenant_sources(40), &["thread_id"]);
    let (prepared, rows) = run(&storage, THREAD_QUERY, &thread_statistics(100)).await;
    assert!(prepared.explain().contains("access=guarded-"), "{}", prepared.explain());
    assert!(
        storage.probe_requests.lock().unwrap().is_empty(),
        "the planner chose; nothing to probe"
    );
    assert_eq!(*storage.bounded_seek_limits.lock().unwrap(), [1_000]);
    let [report] = reports(&storage).try_into().expect("one report");
    assert_eq!(report.kind, AccessPathKind::Equality);
    assert_eq!(report.property.as_deref(), Some("thread_id"));
    assert_eq!((report.estimated_rows, report.entries_read, report.declined), (100, 40, false));

    // Without statistics the same query is probed, and returns the same rows.
    let unranked = probing_storage(tenant_sources(40), &["thread_id"]);
    let (_, expected) = run(&unranked, THREAD_QUERY, &UnknownStatistics).await;
    assert_eq!(rows, expected);
    assert_eq!(unranked.probe_requests.lock().unwrap().len(), 1);
    assert!(reports(&unranked).is_empty());
}

#[tokio::test]
async fn a_stale_equality_declines_and_the_probe_then_runs() {
    // Statistics say one vertex; the thread holds 600, past the 256 floor.
    let storage = probing_storage(tenant_sources(600), &["thread_id"]);
    let (_, rows) = run(&storage, THREAD_QUERY, &thread_statistics(1)).await;
    let (_, expected) = run(&probing_storage(tenant_sources(600), &[]), THREAD_QUERY, &UnknownStatistics).await;
    assert_eq!(rows, expected);
    assert_eq!(rows.len(), 600);
    let [report] = reports(&storage).try_into().expect("one report");
    assert!(report.declined);
    // This backend judges a whole seek, so it really read all 600 entries,
    // and the report says so rather than assuming the limit plus one.
    assert_eq!((report.estimated_rows, report.entries_read), (1, 600));
    // The fallback is today's plan, so today's probe narrows it.
    let probes = storage.probe_requests.lock().unwrap();
    assert_eq!(probes.len(), 1, "{probes:?}");
    assert!(probes[0].iter().any(|candidate| candidate.property == "thread_id"));
}

#[tokio::test]
async fn a_declined_guard_whose_probe_declines_runs_todays_access() {
    let storage = probing_storage(tenant_sources(600), &[]);
    let (_, rows) = run(&storage, THREAD_QUERY, &thread_statistics(1)).await;
    let (_, expected) = run(&probing_storage(tenant_sources(600), &[]), THREAD_QUERY, &UnknownStatistics).await;
    assert_eq!(rows, expected);
    assert_eq!(storage.probe_requests.lock().unwrap().len(), 1);
    // The guarded seek on thread_id, then the fallback's seek on tenant_id.
    let seeks = storage.seek_values.lock().unwrap();
    assert_eq!(*seeks, [string("hot"), string("t")]);
}

#[tokio::test]
async fn a_guarded_label_scan_declines_when_the_label_grew() {
    // The label looked smaller than the tenant; it now holds 600 vertices.
    let statistics = RankingStatistics::default().label("Source", 10).property("tenant_id", 500_000, 1);
    let query = "MATCH (s:Source {tenant_id: 't'}) RETURN s";
    let storage = probing_storage(tenant_sources(600), &[]);
    let (prepared, rows) = run(&storage, query, &statistics).await;
    assert!(prepared.explain().contains("label=Source"), "{}", prepared.explain());
    let (_, expected) = run(&probing_storage(tenant_sources(600), &[]), query, &UnknownStatistics).await;
    assert_eq!(rows, expected);
    assert_eq!(*storage.label_scan_limits.lock().unwrap(), [Some(257)]);
    let [report] = reports(&storage).try_into().expect("one report");
    assert_eq!(report.kind, AccessPathKind::LabelScan);
    assert_eq!(report.property, None);
    assert!(report.declined);

    // Inside its limit, the scan answers alone.
    let small = probing_storage(tenant_sources(20), &[]);
    let (_, rows) = run(&small, query, &statistics).await;
    assert_eq!(rows.len(), 20);
    assert!(!reports(&small)[0].declined);
    assert!(small.seek_values.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_declined_range_seek_is_probed_like_todays_plan() {
    // The range looks like one entry and holds 600; statistics think the
    // provider splits the scope in two, so only the probe can find it narrow.
    // (A provider statistics cannot estimate at all would leave the binding
    // unranked, so no range seek would run.)
    let query = "MATCH (s:Source {tenant_id: 't', app_provider: 'slack'}) WHERE s.created_at >= 0 AND s.created_at < 100 RETURN s";
    let statistics = optimistic_statistics().property("app_provider", 500_000, 2);
    let storage = probing_storage(tenant_sources(600), &["app_provider"]);
    let (prepared, rows) = run(&storage, query, &statistics).await;
    assert!(prepared.explain().contains("access=property-range"), "{}", prepared.explain());
    let (_, expected) = run(&probing_storage(tenant_sources(600), &[]), query, &UnknownStatistics).await;
    assert_eq!(rows, expected);
    assert_eq!(rows.len(), 200);
    assert!(reports(&storage)[0].declined);
    assert_eq!(storage.probe_requests.lock().unwrap().len(), 1);

    // A range inside its limit is not probed.
    let narrow = probing_storage(tenant_sources(100), &["app_provider"]);
    run(&narrow, query, &statistics).await;
    assert!(!reports(&narrow)[0].declined);
    assert!(narrow.probe_requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_seeded_guarded_access_never_probes() {
    let mut vertices = tenant_sources(600);
    vertices.push(VertexRecord::new(9_000).with_label("Other"));
    let query = "MATCH (a:Other) OPTIONAL MATCH (s:Source {tenant_id: 't'}) WHERE s.thread_id = 'hot' RETURN s";
    for per_thread in [100, 1] {
        let storage = probing_storage(vertices.clone(), &["thread_id"]);
        let (prepared, rows) = run(&storage, query, &thread_statistics(per_thread)).await;
        assert!(prepared.explain().contains("access=guarded-"), "{}", prepared.explain());
        let (_, expected) = run(&probing_storage(vertices.clone(), &[]), query, &UnknownStatistics).await;
        assert_eq!(rows, expected, "{per_thread}");
        assert!(storage.probe_requests.lock().unwrap().is_empty(), "{per_thread}");
    }
}

#[tokio::test]
async fn a_guarded_anchor_under_a_join_reaches_the_backend_through_the_hydration_map() {
    let vertices = tenant_sources(40)
        .into_iter()
        .chain([VertexRecord::new(500).with_label("Entity")])
        .collect::<Vec<_>>();
    let relationships = [
        hydradb_cypher_engine::RelationshipRecord::new(1, 3, 500, "RELATES"),
        hydradb_cypher_engine::RelationshipRecord::new(2, 7, 500, "RELATES"),
    ];
    let query = "MATCH (s:Source {tenant_id: 't'})-[:RELATES]->(b:Entity) WHERE s.thread_id = 'hot' RETURN b";
    let storage = MemoryStorage {
        selective_properties: vec!["thread_id".into()],
        ..MemoryStorage::with_graph(vertices.clone(), relationships.clone())
    };
    let (prepared, rows) = run(&storage, query, &thread_statistics(100)).await;
    assert!(prepared.explain().contains("access=guarded-"), "{}", prepared.explain());
    let (_, expected) = run(&MemoryStorage::with_graph(vertices, relationships), query, &UnknownStatistics).await;
    assert_eq!(rows, expected);
    assert_eq!(rows, [500, 500]);
    assert_eq!(*storage.bounded_seek_limits.lock().unwrap(), [1_000]);
    assert!(storage.probe_requests.lock().unwrap().is_empty());
}

#[test]
fn the_guarded_access_renders_and_redacts_statistics_in_its_shape() {
    let (parameters, lists) = corpus_parameters();
    let prepared = prepare(CORPUS[1], &parameters, &lists, &example_scope());
    let explain = prepared.explain();
    assert!(
        explain.contains(
            "VertexScan(provider=slatedb, access=guarded-property, binding=s, labels=[Source], property=thread_id, values=[$thread_id], estimated_rows=100, read_limit=1000)"
        ),
        "{explain}"
    );
    let shape = prepared.plan_shape();
    assert!(shape.contains("estimated_rows=?, read_limit=?"), "{shape}");
    let refreshed = prepare(CORPUS[1], &parameters, &lists, &example_scope().property("thread_id", 500_000, 50));
    assert_ne!(prepared.explain(), refreshed.explain());
    assert_eq!(prepared.plan_hash(), refreshed.plan_hash());
    // Literal values never reach a shape.
    let literal = prepare(THREAD_QUERY, &parameters, &lists, &example_scope());
    assert!(literal.explain().contains("'hot'"), "{}", literal.explain());
    assert!(!literal.plan_shape().contains("hot"), "{}", literal.plan_shape());
}

// ---- An equality statistics cannot estimate ---------------------------------

/// The staging request this guards against: the tenant filters are known to
/// match every Actor, `actor_id` is missing from the published statistics.
const ACTOR_QUERY: &str = "MATCH (a:Actor) WHERE a.actor_id = 'x' AND a.tenant_id = 't' AND a.sub_tenant_id = 'st' RETURN a";
const ACTOR_QUERY_TENANT_FIRST: &str = "MATCH (a:Actor) WHERE a.tenant_id = 't' AND a.sub_tenant_id = 'st' AND a.actor_id = 'x' RETURN a";
/// The tenant filters as an inline map, which lowers to a Filter of its own
/// below the `WHERE`.
const ACTOR_QUERY_INLINE: &str = "MATCH (a:Actor {tenant_id: 't', sub_tenant_id: 'st'}) WHERE a.actor_id = 'x' RETURN a";

fn actor_statistics() -> RankingStatistics {
    RankingStatistics::default()
        .label("Actor", 124_836)
        .property("tenant_id", 124_836, 1)
        .property("sub_tenant_id", 124_836, 1)
}

/// The property of an unguarded single-value seek, or a panic naming the plan.
fn plain_seek(plan: &GraphPhysicalPlan) -> String {
    seek_property(plan, "property-point")
}

#[test]
fn an_unestimated_equality_leaves_the_binding_to_the_unranked_planner() {
    for query in [ACTOR_QUERY, ACTOR_QUERY_TENANT_FIRST, ACTOR_QUERY_INLINE] {
        let explain = explain_corpus(query, &actor_statistics());
        assert!(!explain.contains("access=guarded-"), "{explain}");
        assert_eq!(explain, explain_corpus(query, &UnknownStatistics), "{query}");
    }
    // The written order anchors, as without statistics; the runtime probe
    // then weighs every equality.
    assert_eq!(plain_seek(&corpus_access(ACTOR_QUERY, &actor_statistics())), "actor_id");
    assert_eq!(
        plain_seek(&corpus_access(ACTOR_QUERY_TENANT_FIRST, &actor_statistics())),
        "tenant_id"
    );
    // The inline map is the innermost layer, so it anchors.
    assert_eq!(plain_seek(&corpus_access(ACTOR_QUERY_INLINE, &actor_statistics())), "tenant_id");
}

#[test]
fn every_equality_estimated_still_ranks_the_smallest_first() {
    let statistics = actor_statistics().property("actor_id", 124_836, 124_836);
    for query in [ACTOR_QUERY, ACTOR_QUERY_TENANT_FIRST, ACTOR_QUERY_INLINE] {
        let plan = corpus_access(query, &statistics);
        assert_eq!(guarded_seek(&plan), "actor_id", "{query}");
        assert_eq!(vertex_scan(&plan).properties().estimated_rows, Some(1), "{plan:?}");
    }
}

#[test]
fn an_unseekable_equality_without_an_estimate_does_not_stop_ranking() {
    // NaN is never a seek candidate, so its missing estimate is no reason to
    // give up on the ones that have one.
    let statistics = actor_statistics().property("actor_id", 124_836, 124_836);
    let query = "MATCH (a:Actor) WHERE a.score = $nan AND a.tenant_id = 't' AND a.actor_id = 'x' RETURN a";
    assert_eq!(guarded_seek(&corpus_access(query, &statistics)), "actor_id");
}

#[test]
fn an_integer_no_index_key_holds_is_skipped_not_declined_over() {
    // Declining would hand the stack to the unranked planner, which seeks the
    // first equality -- one the storage adapter cannot encode. Skipped, the
    // broad tenant still loses to the small label, as before.
    let statistics = RankingStatistics::default().label("Actor", 1_000).property("tenant_id", 500_000, 1);
    let query = "MATCH (a:Actor) WHERE a.score = $too_large AND a.tenant_id = 't' RETURN a";
    let plan = corpus_access(query, &statistics);
    assert_eq!(vertex_scan(&plan).access_kind(), "guarded-label", "{plan:?}");
    // The widest integer a key holds is still seekable, so its missing
    // estimate declines ranking.
    let query = "MATCH (a:Actor) WHERE a.score = $fits AND a.tenant_id = 't' RETURN a";
    assert_eq!(plain_seek(&corpus_access(query, &statistics)), "score");
}

#[tokio::test]
async fn an_unestimated_equality_is_found_by_the_probe() {
    let vertices = (1..=300)
        .map(|id| {
            VertexRecord::new(id)
                .with_label("Actor")
                .with_property("tenant_id", string("t"))
                .with_property("sub_tenant_id", string("st"))
                .with_property("actor_id", string(if id == 7 { "x" } else { "other" }))
        })
        .collect::<Vec<_>>();
    // Both shapes anchor on the tenant, so only the probe finds the actor.
    for query in [ACTOR_QUERY_TENANT_FIRST, ACTOR_QUERY_INLINE] {
        let storage = probing_storage(vertices.clone(), &["actor_id"]);
        let (prepared, rows) = run(&storage, query, &actor_statistics()).await;
        assert!(!prepared.explain().contains("access=guarded-"), "{}", prepared.explain());
        assert_eq!(rows, [7], "{query}");
        assert!(reports(&storage).is_empty(), "no statistics-chosen access ran: {query}");
        let probes = storage.probe_requests.lock().unwrap();
        assert_eq!(probes.len(), 1, "{query}: {probes:?}");
        assert!(probes[0].iter().any(|candidate| candidate.property == "actor_id"), "{query}");
        // The probe answered, so the tenant seek never ran.
        assert!(storage.seek_values.lock().unwrap().is_empty(), "{query}");
    }
}
