mod common;

use std::sync::{Arc, Mutex};

use hydradb_cypher_ast::GqlStatus;
use hydradb_cypher_engine::{CypherEngine, ParameterMap, QueryValue, ScalarValue, StorageError};

use common::MemoryStorage;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum Stop {
    #[error("query cancelled")]
    Cancelled,
    #[error("query deadline exhausted")]
    Deadline,
}

const PHASES: [(&str, usize); 6] = [
    ("cypher_composite_materialize", 9),
    ("cypher_composite_counts", 3),
    ("cypher_composite_distinct", 6),
    ("cypher_composite_distinct_rows", 4),
    ("cypher_composite_filter", 4),
    ("cypher_composite_output", 2),
];

#[tokio::test]
async fn composition_checks_each_phase_after_earlier_work_succeeds() {
    for binding in [false, true] {
        for operator in ["EXCEPT", "EXCEPT ALL", "INTERSECT", "INTERSECT ALL"] {
            let source = format!("FOR x IN [3,1,3,2,1,5] RETURN x AS v {operator} FOR y IN [1,3,4] RETURN y AS v");
            let source = if binding {
                format!("RETURN EXISTS {{ {source} }} AS v")
            } else {
                source
            };
            for (phase_index, &(target, _)) in PHASES.iter().enumerate() {
                if operator.ends_with("ALL") && target.contains("distinct") {
                    continue;
                }
                // Materialize all of the first operand before stopping inside the second.
                let stop_at = if phase_index == 0 { 8 } else { 2 };
                for stop in [Stop::Cancelled, Stop::Deadline] {
                    let trace = Arc::new(Mutex::new(Vec::new()));
                    let observed = Arc::clone(&trace);
                    let engine = CypherEngine::new(MemoryStorage {
                        checkpoint: Some(Arc::new(move |operation| {
                            let mut trace = observed.lock().unwrap();
                            trace.push(operation);
                            if operation == target && trace.iter().filter(|&&op| op == target).count() == stop_at {
                                return Err(StorageError::from_backend(stop));
                            }
                            Ok(())
                        })),
                        ..MemoryStorage::default()
                    });
                    let prepared = engine.prepare(&source, &ParameterMap::new()).unwrap();
                    let error = engine.execute_prepared(&prepared).await.expect_err(&source);
                    assert_eq!(error.into_backend_error::<Stop>().unwrap(), stop, "{source}: {target}");
                    let trace = trace.lock().unwrap();
                    assert_eq!(trace.last(), Some(&target));
                    assert_eq!(trace.iter().filter(|&&op| op == target).count(), stop_at);
                    for &(earlier, count) in &PHASES[..phase_index] {
                        if operator.ends_with("ALL") && earlier.contains("distinct") {
                            continue;
                        }
                        let expected = if earlier == "cypher_composite_filter" && operator.ends_with("ALL") {
                            6
                        } else {
                            count
                        };
                        assert_eq!(
                            trace.iter().filter(|&&op| op == earlier).count(),
                            expected,
                            "{source}: {target}: {earlier}"
                        );
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn checked_composition_preserves_bags_and_left_order() {
    // ISO/IEC 39075 §14.2 GR2: DISTINCT uses membership; ALL uses M-N or min(M,N).
    let engine = CypherEngine::new(MemoryStorage::default());
    for (operator, expected) in [
        ("EXCEPT", vec![2, 5]),
        ("EXCEPT ALL", vec![3, 2, 5]),
        ("INTERSECT", vec![3, 1]),
        ("INTERSECT ALL", vec![3, 1, 1]),
    ] {
        let source = format!("FOR x IN [3,1,3,2,1,5] RETURN x AS v {operator} FOR y IN [1,1,3,4] RETURN y AS v");
        let prepared = engine.prepare(&source, &ParameterMap::new()).unwrap();
        let result = engine.execute_prepared(&prepared).await.unwrap();
        let actual = result
            .rows
            .iter()
            .map(|row| match row.values.as_slice() {
                [QueryValue::Scalar(ScalarValue::Integer(value))] => *value,
                other => panic!("{other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "{source}");
    }
    let source = "FOR x IN [3,1,3,2,1,5] RETURN x AS v EXCEPT ALL FOR y IN [1,1,3,4] RETURN y AS v EXCEPT ALL RETURN 3 AS v";
    let result = engine
        .execute_prepared(&engine.prepare(source, &ParameterMap::new()).unwrap())
        .await
        .unwrap();
    assert_eq!(common::compat::table(&result), [["2"], ["5"]]);
}

#[tokio::test]
async fn checked_otherwise_keeps_empty_and_lazy_semantics() {
    for binding in [false, true] {
        for (source, expected_rows, expected_materialized) in [
            ("FOR x IN [null,null] RETURN x AS v OTHERWISE RETURN 1/0 AS v", 2, 2),
            ("FOR x IN [] RETURN x AS v OTHERWISE FOR y IN [2,2,3] RETURN y AS v", 3, 3),
            ("FOR x IN [] RETURN x AS v OTHERWISE FOR y IN [] RETURN y AS v", 0, 0),
        ] {
            let source = if binding {
                format!("RETURN EXISTS {{ {source} }} AS v")
            } else {
                source.to_string()
            };
            let trace = Arc::new(Mutex::new(Vec::new()));
            let observed = Arc::clone(&trace);
            let engine = CypherEngine::new(MemoryStorage {
                checkpoint: Some(Arc::new(move |operation| {
                    observed.lock().unwrap().push(operation);
                    Ok(())
                })),
                ..MemoryStorage::default()
            });
            let prepared = engine.prepare(&source, &ParameterMap::new()).unwrap();
            let result = engine.execute_prepared(&prepared).await.unwrap();
            if binding {
                assert_eq!(result.rows[0].values, [QueryValue::Scalar(ScalarValue::Boolean(expected_rows > 0))]);
            } else {
                assert_eq!(result.rows.len(), expected_rows);
            }
            let trace = trace.lock().unwrap();
            assert_eq!(trace.iter().filter(|&&op| op == PHASES[0].0).count(), expected_materialized);
            assert_eq!(trace.iter().filter(|&&op| op == PHASES[5].0).count(), expected_rows);
            assert!(trace.iter().all(|op| !PHASES[1..5].iter().any(|&(phase, _)| phase == *op)));
        }
    }
}

#[tokio::test]
async fn otherwise_still_checks_materialization_and_output() {
    for binding in [false, true] {
        for source in [
            "FOR x IN [1,2,3] RETURN x AS v OTHERWISE RETURN 1/0 AS v",
            "FOR x IN [] RETURN x AS v OTHERWISE FOR y IN [1,2,3] RETURN y AS v",
        ] {
            let source = if binding {
                format!("RETURN EXISTS {{ {source} }} AS v")
            } else {
                source.to_string()
            };
            for target in [PHASES[0].0, PHASES[5].0] {
                let calls = Arc::new(Mutex::new(0));
                let engine = CypherEngine::new(MemoryStorage {
                    checkpoint: Some(Arc::new(move |operation| {
                        if operation == target {
                            let mut calls = calls.lock().unwrap();
                            *calls += 1;
                            if *calls == 2 {
                                return Err(StorageError::from_backend(Stop::Cancelled));
                            }
                        }
                        Ok(())
                    })),
                    ..MemoryStorage::default()
                });
                let prepared = engine.prepare(&source, &ParameterMap::new()).unwrap();
                let error = engine.execute_prepared(&prepared).await.unwrap_err();
                assert_eq!(error.into_backend_error::<Stop>().unwrap(), Stop::Cancelled, "{source}: {target}");
            }
        }
    }
}

#[tokio::test]
async fn checked_composition_preserves_semantic_keys_and_arm_errors() {
    let engine = CypherEngine::new(MemoryStorage::default());
    let parameters = ParameterMap::from([("nan".to_string(), ScalarValue::Float("NaN".into()))]);
    for operator in ["EXCEPT", "EXCEPT ALL", "INTERSECT", "INTERSECT ALL"] {
        let source = format!("FOR x IN [$nan,$nan] RETURN x AS v {operator} RETURN $nan AS v");
        let prepared = engine.prepare(&source, &parameters).unwrap();
        let result = engine.execute_prepared(&prepared).await.unwrap();
        assert_eq!(result.rows.len(), if operator.starts_with("EXCEPT") { 2 } else { 0 });
    }
    for (source, expected) in [
        (
            "FOR x IN [[1,null],[1.0,null],[0.0,null],[-0.0,null]] RETURN x AS v EXCEPT RETURN [1.0,null] AS v",
            vec![vec!["[0.0f, null]"]],
        ),
        (
            "FOR x IN [1,1.0,null,null] RETURN x AS v INTERSECT FOR y IN [1.0,null] RETURN y AS v",
            vec![vec!["1"], vec!["null"]],
        ),
    ] {
        let prepared = engine.prepare(source, &ParameterMap::new()).unwrap();
        let result = engine.execute_prepared(&prepared).await.unwrap();
        assert_eq!(common::compat::table(&result), expected, "{source}");
    }
    for binding in [false, true] {
        let source = "RETURN 1 AS v EXCEPT RETURN 1/0 AS v";
        let source = if binding {
            format!("RETURN EXISTS {{ {source} }} AS v")
        } else {
            source.to_string()
        };
        let engine = CypherEngine::new(MemoryStorage {
            checkpoint: Some(Arc::new(|operation| {
                if operation == "cypher_composite_counts" {
                    return Err(StorageError::from_backend(Stop::Deadline));
                }
                Ok(())
            })),
            ..MemoryStorage::default()
        });
        let prepared = engine.prepare(&source, &ParameterMap::new()).unwrap();
        assert_eq!(
            engine.execute_prepared(&prepared).await.unwrap_err().gql_status(),
            GqlStatus::DIVISION_BY_ZERO
        );
    }
}
