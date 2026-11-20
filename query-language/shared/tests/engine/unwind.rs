//! UNWIND and list values.
//!
//! The matrix rows `compat_unwind_*` hold the same answers to Neo4j. Edge
//! semantics below were probed on Neo4j 2026.09 under CYPHER 25: a null list
//! unwinds to no rows, a non-list value to one, and a list element that is
//! null still makes a row.

mod common;

use common::compat::{columns, refusal, run, run_with, table};
use hydradb_cypher_engine::{ListParameterMap, ParameterMap, ScalarValue};

fn list(name: &str, values: &[i128]) -> ListParameterMap {
    ListParameterMap::from([(name.to_string(), values.iter().map(|value| ScalarValue::Integer(*value)).collect())])
}

#[tokio::test]
async fn unwinding_a_list_parameter_makes_a_row_per_element() {
    let result = run_with(
        "UNWIND $values AS value RETURN value",
        &ParameterMap::new(),
        &list("values", &[3, 1, 3]),
    )
    .await;
    assert_eq!(columns(&result), ["value"]);
    assert_eq!(table(&result), rows![["3"], ["1"], ["3"]]);
}

#[tokio::test]
async fn unwinding_a_literal_then_ordering() {
    let result = run("UNWIND [3, 1, 2] AS value RETURN value ORDER BY value DESC").await;
    assert_eq!(table(&result), rows![["3"], ["2"], ["1"]]);
}

#[tokio::test]
async fn null_empty_scalar_and_null_elements() {
    assert!(table(&run("UNWIND null AS x RETURN x").await).is_empty());
    assert!(table(&run("UNWIND [] AS x RETURN x").await).is_empty());
    assert_eq!(table(&run("UNWIND 5 AS x RETURN x").await), rows![["5"]]);
    assert_eq!(table(&run("UNWIND [1, null] AS x RETURN x").await), rows![["1"], ["null"]]);
}

#[tokio::test]
async fn an_unwound_value_correlates_a_following_match() {
    let result = run("UNWIND ['apple', 'carrot'] AS wanted MATCH (i:Item {name: wanted}) RETURN i.score AS score ORDER BY score ASC").await;
    assert_eq!(table(&result), rows![["0"], ["2"], ["3"]]);
}

#[tokio::test]
async fn unwind_after_match_multiplies_rows() {
    let result = run("MATCH (h:Hub) UNWIND [1, 2] AS copy RETURN h.name AS name, copy ORDER BY copy ASC").await;
    assert_eq!(table(&result), rows![["\"hub\"", "1"], ["\"hub\"", "2"]]);
}

#[tokio::test]
async fn two_unwinds_form_a_cross_product() {
    let result = run("UNWIND [1, 2] AS x UNWIND [10, 20] AS y RETURN x, y").await;
    assert_eq!(table(&result), rows![["1", "10"], ["1", "20"], ["2", "10"], ["2", "20"]]);
}

#[tokio::test]
async fn an_unwind_element_can_be_filtered_and_aggregated() {
    let result = run("UNWIND [1, 2, 3, 4] AS x WITH x WHERE x % 2 = 0 RETURN count(*) AS evens, sum(x) AS total").await;
    assert_eq!(table(&result), rows![["2", "6"]]);
}

#[tokio::test]
async fn nested_lists_and_list_values_return_as_lists() {
    let result = run("UNWIND [[1, 2], [3]] AS x RETURN x, size(x) AS n").await;
    assert_eq!(table(&result), rows![["[1, 2]", "2"], ["[3]", "1"]]);
    assert_eq!(table(&run("RETURN [1, 'a', null] AS l").await), rows![["[1, \"a\", null]"]]);
}

#[tokio::test]
async fn list_equality_is_element_wise() {
    let result = run("RETURN [1, 2] = [1, 2] AS same, [1, 2] = [1, 3] AS different, [1] = [1, 2] AS shorter").await;
    assert_eq!(table(&result), rows![["true", "false", "false"]]);
}

#[tokio::test]
async fn an_unwind_cannot_rebind_a_name_in_scope() {
    assert!(refusal("MATCH (i:Item) UNWIND [1] AS i RETURN i").await.contains("cannot rebind i"));
}

#[tokio::test]
async fn an_unwound_value_cannot_be_bound_as_a_node() {
    assert!(refusal("UNWIND [1] AS x MATCH (x)-[:R]->(y) RETURN y")
        .await
        .contains("holds a value"));
}

#[tokio::test]
async fn unwind_is_a_valid_contextual_identifier() {
    assert_eq!(table(&run("MATCH (unwind:Item) RETURN count(unwind) AS n").await), rows![["6"]]);
}

#[tokio::test]
async fn nested_constant_unwind_respects_a_small_output_budget() {
    use hydradb_cypher_engine::{explain_physical_plan_shape, CypherEngine};
    let engine = CypherEngine::new(common::MemoryStorage {
        max_intermediate_rows: Some(10),
        ..common::MemoryStorage::default()
    });
    let query = "UNWIND $large AS x UNWIND [1] AS y RETURN x LIMIT 1";
    let parameters = ParameterMap::new();
    let prepared = engine
        .prepare_with_lists(query, &parameters, &list("large", &(0..100).collect::<Vec<_>>()))
        .unwrap();
    assert_eq!(table(&engine.execute_prepared(&prepared).await.unwrap()), rows![["0"]]);
    let small = engine.prepare_with_lists(query, &parameters, &list("large", &[0])).unwrap();
    assert_eq!(
        explain_physical_plan_shape(&prepared.physical),
        explain_physical_plan_shape(&small.physical)
    );
}

#[tokio::test]
async fn dependent_empty_lists_do_not_hide_later_nonempty_inputs() {
    assert_eq!(
        table(&run("UNWIND [[], [2]] AS xs UNWIND xs AS x RETURN x LIMIT 1").await),
        rows![["2"]]
    );
}

#[tokio::test]
async fn redacted_lists_retain_property_dependencies() {
    use hydradb_cypher_engine::{explain_physical_plan_shape, CypherEngine};
    let engine = CypherEngine::new(common::compat::graph());
    let a = engine
        .prepare("MATCH (i:Item) UNWIND [i.name] AS x RETURN x", &ParameterMap::new())
        .unwrap();
    let b = engine
        .prepare("MATCH (i:Item) UNWIND [i.score] AS x RETURN x", &ParameterMap::new())
        .unwrap();
    assert_ne!(explain_physical_plan_shape(&a.physical), explain_physical_plan_shape(&b.physical));
}

#[tokio::test]
async fn list_relational_comparisons_are_lexicographic_and_three_valued() {
    assert_eq!(
        table(
            &run("RETURN [1]<[2] AS a, [1,null]<[1,2] AS b, [1]<[1,null] AS c, [] < [] AS d, [[1]] < [[2]] AS e, [(1=1)] <= [true] AS f")
                .await
        ),
        rows![["true", "null", "true", "false", "true", "true"]]
    );
    assert_eq!(
        table(&run("UNWIND [[2],[1],[3]] AS x WITH x WHERE x < [3] RETURN x ORDER BY x").await),
        rows![["[1]"], ["[2]"]]
    );
}

#[tokio::test]
async fn list_set_operators_use_numeric_equality_recursively() {
    assert_eq!(
        table(&run("UNWIND [[1],[1.0],[2]] AS x RETURN DISTINCT x ORDER BY x").await),
        rows![["[1]"], ["[2]"]]
    );
    assert_eq!(
        table(&run("UNWIND [1,1.0,2] AS x RETURN x AS key, count(*) AS n ORDER BY key").await),
        rows![["1", "2"], ["2", "1"]]
    );
}

#[tokio::test]
async fn id_stream_becomes_a_scan_preserving_order_duplicates_and_scalar_projection() {
    use hydradb_cypher_engine::CypherEngine;
    let storage = common::compat::graph();
    let requests = storage.expand_requests.clone();
    let engine = CypherEngine::new(storage);
    let prepared = engine
        .prepare(
            "UNWIND [1020,1021,1020,9999] AS src MATCH (a {id:src})-[:R]->(b) RETURN src, b.id AS dst",
            &ParameterMap::new(),
        )
        .unwrap();
    let explain = prepared.explain();
    assert!(explain.contains("occurrences=preserved"), "{explain}");
    assert!(!explain.contains("NaturalJoin") && !explain.contains("Unwind"), "{explain}");
    let result = engine.execute_prepared(&prepared).await.unwrap();
    assert_eq!(columns(&result), ["src", "dst"]);
    assert_eq!(
        table(&result),
        rows![
            ["1020", "#1021"],
            ["1020", "#1022"],
            ["1021", "#1023"],
            ["1020", "#1021"],
            ["1020", "#1022"]
        ]
    );
    assert!(matches!(
        result.rows[0].values[0],
        hydradb_cypher_engine::QueryValue::Scalar(ScalarValue::Integer(1020))
    ));
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].input_vertex_ids, [1020, 1021]);
}

#[tokio::test]
async fn scalar_id_unwind_rewrites_filtered_and_named_paths() {
    use hydradb_cypher_engine::{plan_physical, CypherEngine, PreparedQuery, UnknownStatistics};
    let engine = CypherEngine::new(common::compat::graph());
    for query in [
        "UNWIND $ids AS src MATCH (a {id:src})-[:R]->(b:Node {name:'b'}) RETURN src,b.name",
        "UNWIND $ids AS src MATCH p=(a {id:src})-[:R]->(b:Node {name:'b'}) RETURN src,b.name,p",
        "UNWIND $ids AS src MATCH (a {id:src})-[:R*1..1]->(b:Node {name:'b'}) RETURN src,b.name",
        "UNWIND $ids AS src MATCH (a {id:src})-[:R]->(b:Node {name:'b'}) MATCH (b)-[:R]->(c:Node {name:'d'}) RETURN src,c.name",
    ] {
        let lowered = engine
            .lower_with_lists(query, &ParameterMap::new(), &list("ids", &[1020, 1021, 1020, 9999]))
            .unwrap();
        let original = PreparedQuery {
            ast: lowered.ast.clone(),
            logical: lowered.logical.clone(),
            physical: plan_physical(&lowered.logical, lowered.planning_context()).unwrap(),
        };
        assert!(original.explain().contains("Unwind"), "{query}");
        let rewritten = lowered.plan_with_statistics(&UnknownStatistics).unwrap();
        let explain = rewritten.explain();
        assert!(
            !explain.contains("Unwind") && !explain.contains("NaturalJoin"),
            "{query}: {explain}"
        );
        assert!(explain.contains("occurrences=preserved"), "{query}: {explain}");
        let mut expected = engine.execute_prepared(&original).await.unwrap();
        let mut actual = engine.execute_prepared(&rewritten).await.unwrap();
        assert_eq!(actual.columns, expected.columns, "{query}");
        assert_eq!(actual.rows.len(), 2, "{query}");
        assert!(
            actual
                .rows
                .iter()
                .all(|row| matches!(row.values[0], hydradb_cypher_engine::QueryValue::Scalar(ScalarValue::Integer(1020)))),
            "{query}"
        );
        actual.rows.sort_by_cached_key(|row| format!("{row:?}"));
        expected.rows.sort_by_cached_key(|row| format!("{row:?}"));
        assert_eq!(actual.rows, expected.rows, "{query}");
    }
}

#[tokio::test]
async fn id_stream_parameters_rebind_every_time() {
    use hydradb_cypher_engine::{CypherEngine, LoweredTemplate, UnknownStatistics};
    let engine = CypherEngine::new(common::compat::graph());
    let template = LoweredTemplate::lower("UNWIND $ids AS src MATCH (a {id:src})-[:R]->(b) RETURN src,b.id AS dst").unwrap();
    for (ids, count) in [(vec![1020, 1020], 4), (vec![1021], 1), (vec![], 0)] {
        let prepared = template
            .bind(&ParameterMap::new(), &list("ids", &ids))
            .plan_with_statistics(&UnknownStatistics)
            .unwrap();
        let explain = prepared.explain();
        assert!(!explain.contains("NaturalJoin") && !explain.contains("Unwind"), "{explain}");
        assert_eq!(engine.execute_prepared(&prepared).await.unwrap().rows.len(), count);
    }
}

#[tokio::test]
async fn id_streams_do_not_reinterpret_map_fields_or_stored_id_properties() {
    use hydradb_cypher_engine::{CypherEngine, LoweredTemplate, UnknownStatistics};
    let engine = CypherEngine::new(common::compat::graph());
    // An integer has no record fields, even when the field is spelled `id`.
    for query in [
        "UNWIND $ids AS src MATCH (a {id:src.id}) RETURN src.id",
        "UNWIND $ids AS src MATCH (a {id:src.key}) RETURN src.key",
    ] {
        let prepared = LoweredTemplate::lower(query)
            .unwrap()
            .bind(&ParameterMap::new(), &list("ids", &[1020]))
            .plan_with_statistics(&UnknownStatistics)
            .unwrap();
        assert!(prepared.explain().contains("Unwind"), "{query}");
        assert!(engine.execute_prepared(&prepared).await.unwrap().rows.is_empty(), "{query}");
    }
    let byog = LoweredTemplate::lower_timed_with_id_properties("UNWIND $ids AS src MATCH (a {id:src}) RETURN src", true)
        .unwrap()
        .0
        .bind(&ParameterMap::new(), &list("ids", &[1020]))
        .plan_with_statistics(&UnknownStatistics)
        .unwrap();
    assert!(byog.explain().contains("Unwind"));
}

#[tokio::test]
async fn connected_fixed_hops_are_linear_and_keep_prior_path_multiplicity() {
    use hydradb_cypher_engine::CypherEngine;
    let storage = common::compat::graph();
    let requests = storage.expand_requests.clone();
    let engine = CypherEngine::new(storage);
    let prepared = engine
        .prepare(
            "MATCH (a) WHERE a.id IN [1020,1020] MATCH (a)-[r:R]->(b) MATCH (b)-[s:R]->(c) RETURN a.id,b.id,c.id",
            &ParameterMap::new(),
        )
        .unwrap();
    assert!(!prepared.explain().contains("NaturalJoin"), "{}", prepared.explain());
    let result = engine.execute_prepared(&prepared).await.unwrap();
    assert_eq!(table(&result), rows![["#1020", "#1021", "#1023"], ["#1020", "#1022", "#1023"]]);
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].input_vertex_ids, [1021, 1022]);
}

#[tokio::test]
async fn repeated_id_occurrences_multiply_parallel_relationship_rows() {
    use hydradb_cypher_engine::{CypherEngine, RelationshipRecord, VertexRecord};
    let storage = common::MemoryStorage::with_graph(
        [VertexRecord::new(1), VertexRecord::new(2)],
        [RelationshipRecord::new(100, 1, 2, "R"), RelationshipRecord::new(101, 1, 2, "R")],
    );
    let engine = CypherEngine::new(storage);
    let result = engine
        .execute(
            "UNWIND [1,1] AS src MATCH (a {id:src})-[r:R]->(b) RETURN src,id(r)",
            &ParameterMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(table(&result), rows![["1", "100"], ["1", "101"], ["1", "100"], ["1", "101"]]);
}

#[tokio::test]
async fn rewritten_streams_match_the_original_correlated_plan_across_windows_and_hops() {
    use hydradb_cypher_engine::{plan_physical, CypherEngine, PreparedQuery};
    let engine = CypherEngine::new(common::compat::graph());
    let queries = [
        "UNWIND [1020] AS src MATCH (a {id:src})-[:R]->(b) WHERE a.id = src RETURN b",
        "UNWIND [1020] AS src MATCH (a {id:src}) WHERE a.id = src RETURN src,a.name",
        "UNWIND [1020,1021,1020] AS src MATCH (a {id:src})-[:R]->(b) WHERE a.id = src AND src >= 1020 RETURN src + 1 AS next_id,b.id ORDER BY next_id,b.id",
        "UNWIND [1020,1021] AS src MATCH (a {id:src})-[:R]->(b) RETURN CASE WHEN src = 1020 THEN src ELSE 0 END AS source,a.name,b.name ORDER BY source,b.name",
        "UNWIND [1020,1021,1020] AS src MATCH (a {id:src})-[:R]->(b) RETURN src,count(b) AS n ORDER BY src",
        "UNWIND [1020,1021] AS src MATCH (a {id:src})-[:R]->(b) RETURN sum(src) AS total",
        "UNWIND [1020,1021,1020,9999] AS src MATCH (a {id:src}) RETURN src,a.id LIMIT 2",
        "UNWIND [9999,1020,1021] AS src MATCH (a {id:src})-[:R]->(b) RETURN src,b.id SKIP 1 LIMIT 2",
        "UNWIND [1020,1020] AS src MATCH (a:Node {id:src})-[:R]->(b) RETURN DISTINCT src,b.id",
        "UNWIND [1020,1020] AS src MATCH (a {id:src})-[:R]->(b) RETURN src,b.id LIMIT 0",
        "UNWIND [1020,1020] AS src MATCH (a {id:src})-[:R]->(b), (b)-[:R]->(c) RETURN src,c.id",
        "UNWIND [1024,1024] AS src MATCH (a {id:src})-[r:R]->(b)-[s:R]->(c) RETURN src,id(r),id(s)",
        "UNWIND [1024,1024] AS src MATCH (a {id:src})-[r:R]->(b) MATCH (b)-[s:R]->(c) RETURN src,id(r),id(s)",
        "UNWIND [1020,-1,null,1020] AS src MATCH (a {id:src})-[:R]->(b) RETURN src,b.id",
    ];
    for query in queries {
        let logical = engine
            .lower_with_lists(query, &ParameterMap::new(), &ListParameterMap::new())
            .unwrap();
        let original = PreparedQuery {
            ast: logical.ast.clone(),
            logical: logical.logical.clone(),
            physical: plan_physical(&logical.logical, logical.planning_context()).unwrap(),
        };
        let rewritten = logical.plan_with_statistics(&hydradb_cypher_engine::UnknownStatistics).unwrap();
        let expected = engine.execute_prepared(&original).await.unwrap();
        let actual = engine.execute_prepared(&rewritten).await.unwrap();
        assert_eq!(actual.columns, expected.columns, "{query}");
        assert_eq!(actual.rows, expected.rows, "{query}");
        if !query.contains("-1") {
            assert!(!rewritten.explain().contains("NaturalJoin"), "{query}: {}", rewritten.explain());
        }
    }
}

#[tokio::test]
async fn id_stream_rewrite_does_not_substitute_across_rebinding_or_dropped_sources() {
    use hydradb_cypher_engine::CypherEngine;
    let engine = CypherEngine::new(common::compat::graph());
    for (query, expected) in [
        ("UNWIND [1020] AS src MATCH (a {id:src}) WITH a,999 AS src RETURN src", "999"),
        ("UNWIND [1020] AS src MATCH (a {id:src}) WITH src RETURN src", "1020"),
        ("UNWIND [1020] AS src MATCH (a {id:src}) WITH src,999 AS a RETURN src", "1020"),
    ] {
        let prepared = engine.prepare(query, &ParameterMap::new()).unwrap();
        let explain = prepared.explain();
        assert!(
            !explain.contains("Unwind") && !explain.contains("NaturalJoin"),
            "{query}: {explain}"
        );
        let result = engine.execute_prepared(&prepared).await.unwrap();
        assert_eq!(table(&result), vec![vec![expected.to_string()]], "{query}");
    }
}

#[tokio::test]
async fn an_explicit_lookup_list_preserves_occurrences_with_an_existing_seed() {
    use hydradb_cypher_engine::{plan_physical, BoundValue, CypherEngine, GraphLogicalPlan, PreparedQuery, ValueOrigin};
    let engine = CypherEngine::new(common::compat::graph());
    let lowered = engine
        .lower_with_lists(
            "MATCH (a {id:1020}) MATCH (a) RETURN a.id LIMIT 1",
            &ParameterMap::new(),
            &ListParameterMap::new(),
        )
        .unwrap();
    let mut logical = lowered.logical.as_ref().clone();
    let GraphLogicalPlan::Project { input, .. } = &mut logical else {
        panic!("project")
    };
    let GraphLogicalPlan::Limit { input: join, .. } = input.as_mut() else {
        panic!("limit")
    };
    let GraphLogicalPlan::NaturalJoin { right, .. } = join.as_mut() else {
        panic!("join")
    };
    let GraphLogicalPlan::NodeScan { lookup_ids, .. } = right.as_mut() else {
        panic!("scan")
    };
    *lookup_ids = Some(
        [1021, 1020, 1020]
            .map(|id| BoundValue {
                value: ScalarValue::Integer(id),
                origin: ValueOrigin::Literal,
            })
            .to_vec(),
    );
    let original = PreparedQuery {
        ast: lowered.ast.clone(),
        logical: std::sync::Arc::new(logical.clone()),
        physical: plan_physical(&logical, lowered.planning_context()).unwrap(),
    };
    let result = engine.execute_prepared(&original).await.unwrap();
    assert_eq!(table(&result), rows![["#1020"]]);
    // Removing the result window must expose both matching occurrences.
    let GraphLogicalPlan::Project { input, .. } = &mut logical else {
        unreachable!()
    };
    let GraphLogicalPlan::Limit { input: join, .. } = input.as_ref() else {
        unreachable!()
    };
    *input = join.clone();
    let all = PreparedQuery {
        ast: lowered.ast.clone(),
        logical: std::sync::Arc::new(logical.clone()),
        physical: plan_physical(&logical, lowered.planning_context()).unwrap(),
    };
    assert_eq!(table(&engine.execute_prepared(&all).await.unwrap()), rows![["#1020"], ["#1020"]]);
}

#[tokio::test]
async fn id_streams_respect_scalar_and_reserved_parameter_precedence() {
    use hydradb_cypher_engine::{CypherEngine, REQUEST_TIMESTAMP_PARAMETER, SESSION_USER_PARAMETER};
    let engine = CypherEngine::new(common::compat::graph());
    let query = "UNWIND $ids AS src MATCH (a {id:src})-[:R]->(b) RETURN src,b.id";
    let prepared = engine
        .prepare_with_lists(
            query,
            &ParameterMap::from([("ids".into(), ScalarValue::Integer(1021))]),
            &list("ids", &[1020]),
        )
        .unwrap();
    assert!(prepared.explain().contains("Unwind"));
    assert_eq!(table(&engine.execute_prepared(&prepared).await.unwrap()), rows![["1021", "#1023"]]);
    for parameter in [SESSION_USER_PARAMETER, REQUEST_TIMESTAMP_PARAMETER] {
        let prepared = engine
            .prepare_with_lists(
                &format!("UNWIND $`{parameter}` AS src MATCH (a {{id:src}}) RETURN src"),
                &ParameterMap::new(),
                &list(parameter, &[1020]),
            )
            .unwrap();
        assert!(prepared.explain().contains("Unwind"));
        assert!(engine.execute_prepared(&prepared).await.unwrap().rows.is_empty());
    }
}
