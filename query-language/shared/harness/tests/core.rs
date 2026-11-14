use hydradb_cypher_tck_harness::{
    compare::{rows_equal, value_eq, Order},
    importer::{expand, parse},
    values::Value,
};

#[test]
fn background_and_outline_are_expanded_without_state_leakage() {
    let f = parse("@feature\nFeature: example\n  Background:\n    Given an empty graph\n  @scenario\n  Scenario Outline: case <n>\n    When executing query:\n      \"\"\"\n      RETURN <n>\n      \"\"\"\n    Then the result should be, in any order:\n      | value |\n      | <n> |\n    @examples\n    Examples:\n      | n |\n      | 1 |\n      | 2 |\n").unwrap();
    let cases = expand(&f, "local", "rev", "a.feature").unwrap();
    assert_eq!(cases.len(), 2);
    assert_ne!(cases[0].id, cases[1].id);
    assert_eq!(cases[0].steps[0].text, "an empty graph");
    assert_eq!(cases[1].steps[1].docstring.as_deref(), Some("\nRETURN 2\n"));
    assert_eq!(cases[0].steps[2].table.as_ref().unwrap()[1][0], "1");
    assert!(cases[0].tags.contains(&"examples".to_string()));
}
#[test]
fn comparison_respects_multiplicity_and_nested_list_order() {
    let one = Value::Integer(1);
    let two = Value::Integer(2);
    let a = Value::List(vec![one.clone(), two.clone()]);
    let b = Value::List(vec![two.clone(), one.clone()]);
    assert!(!value_eq(&a, &b, Order::Ordered));
    assert!(value_eq(&a, &b, Order::Unordered));
    let c = vec!["x".to_owned()];
    assert!(!rows_equal(
        &c,
        &[vec![one.clone()], vec![one.clone()]],
        &c,
        &[vec![one], vec![two]],
        Order::Unordered,
        Order::Ordered
    ));
}
#[test]
fn numeric_comparison_does_not_round_large_integers() {
    assert!(!value_eq(
        &Value::Integer(9007199254740993),
        &Value::Float((9007199254740992_f64).to_bits()),
        Order::Ordered
    ));
    assert!(value_eq(
        &Value::Float(f64::NAN.to_bits()),
        &Value::Float(f64::NAN.to_bits()),
        Order::Ordered
    ));
    assert!(!value_eq(
        &Value::Integer(i128::MAX),
        &Value::Float((i128::MAX as f64).to_bits()),
        Order::Ordered
    ));
    assert!(value_eq(
        &Value::Integer(i128::MIN),
        &Value::Float((i128::MIN as f64).to_bits()),
        Order::Ordered
    ));
}

#[test]
#[ignore = "requires a separately fetched pinned corpus"]
fn pinned_step_inventory() {
    use hydradb_cypher_tck_harness::steps::bind;
    use std::{fs, path::Path};
    fn walk(
        path: &Path,
        count: &mut usize,
        definitions: &mut usize,
        ids: &mut std::collections::BTreeSet<hydradb_cypher_tck_harness::importer::CaseId>,
    ) {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, count, definitions, ids);
                continue;
            }
            if path.extension().is_none_or(|e| e != "feature") {
                continue;
            }
            let text = fs::read_to_string(&path).unwrap();
            let feature = parse(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            *definitions += feature.scenarios.len();
            let cases = expand(
                &feature,
                "official",
                "677cbafabb8c3c5eed458fd3b1ec0daec8d67d23",
                path.to_str().unwrap(),
            )
            .unwrap();
            for case in cases {
                assert!(ids.insert(case.id.clone()), "duplicate case identity");
                hydradb_cypher_tck_harness::steps::bind_scenario(&case)
                    .unwrap_or_else(|e| panic!("{}:{}: {e}", path.display(), case.id.scenario_line));
                let kinds: Vec<_> = case
                    .steps
                    .iter()
                    .map(|step| bind(step).unwrap_or_else(|e| panic!("{}:{}: {e}", path.display(), step.line)))
                    .collect();
                hydradb_cypher_tck_harness::lifecycle::validate(&kinds)
                    .unwrap_or_else(|e| panic!("{}:{}: {e}", path.display(), case.id.scenario_line));
            }
            *count += 1;
        }
    }
    let root = std::env::var("TCK_FEATURES").expect("set TCK_FEATURES to the pinned feature tree");
    let mut count = 0;
    let mut definitions = 0;
    let mut ids = std::collections::BTreeSet::new();
    walk(Path::new(&root), &mut count, &mut definitions, &mut ids);
    assert_eq!(count, 220);
    assert_eq!(definitions, 1615);
    assert!(ids.len() >= definitions);
}

#[test]
fn table_compatibility_preserves_unknown_escapes_and_positions() {
    let source = "Feature: escape\n  Scenario: values\n    Then the result should be, in any order:\n      | literal |\n      | '\\'' |\n      | 'a\\|b' |\n      | 'a\\\\b' |\n";
    let feature = parse(source).unwrap();
    let table = feature.scenarios[0].steps[0].table.as_ref().unwrap();
    assert_eq!(table.position.line, 4);
    assert_eq!(table.rows[1][0], "'\\''");
    assert_eq!(table.rows[2][0], "'a|b'");
    assert_eq!(table.rows[3][0], "'a\\b'");
}

#[test]
fn policy_rejects_unexpected_pass_and_exact_signature_change() {
    use hydradb_cypher_tck_harness::policy::*;
    use std::collections::{BTreeMap, BTreeSet};
    let d = Divergence {
        corpus_revision: "rev".into(),
        case_id: "case".into(),
        engine: "legacy".into(),
        layer: "execution".into(),
        expected_signature: "wrong rows".into(),
        reason: "reason".into(),
        owner: "owner".into(),
        reference: "issue".into(),
        review_by: "2028-12-01".into(),
    };
    let policy = Divergences {
        schema_version: 1,
        divergences: vec![d],
    };
    let key = Key {
        case_id: "case".into(),
        engine: "legacy".into(),
        layer: "execution".into(),
    };
    let cases = BTreeSet::from(["case".into()]);
    assert!(validate(
        &policy,
        "rev",
        "2028-10-28",
        &cases,
        &BTreeMap::from([(key.clone(), Outcome::Fail("wrong rows".into()))])
    )
    .is_empty());
    assert!(!validate(&policy, "rev", "2028-10-28", &cases, &BTreeMap::from([(key, Outcome::Pass)])).is_empty());
}

#[test]
fn graph_effects_count_replacement_and_distinct_labels() {
    use hydradb_cypher_tck_harness::lifecycle::{effects, GraphState};
    let mut before = GraphState::default();
    before.nodes.insert("n1".into());
    before.labels.insert(("n1".into(), "L".into()));
    before.properties.insert(("n1".into(), "x".into()), Value::Integer(1));
    let mut after = before.clone();
    after.nodes.insert("n2".into());
    after.labels.insert(("n2".into(), "L".into()));
    after.properties.insert(("n1".into(), "x".into()), Value::Integer(2));
    let d = effects(&before, &after);
    assert_eq!(
        (d.nodes_added, d.properties_added, d.properties_removed, d.labels_added),
        (1, 1, 1, 1)
    );
}

#[test]
fn reports_are_sorted_and_escape_xml() {
    use hydradb_cypher_tck_harness::report::{Record, Report, Verdict};
    let record = |id: &str| Record {
        case_id: id.into(),
        engine: "e&1".into(),
        step: 0,
        layer: "parser".into(),
        verdict: Verdict::Fail,
        evidence: serde_json::json!({"error":"<bad>"}),
    };
    let r = Report {
        schema_version: 1,
        corpus_revision: "rev".into(),
        repository_revision: "repo".into(),
        records: vec![record("z"), record("a")],
    };
    let mut reverse = r.clone();
    reverse.records.reverse();
    assert_eq!(r.jsonl().unwrap(), reverse.jsonl().unwrap());
    assert_eq!(r.junit(), reverse.junit());
    assert!(r.junit().contains("e&amp;1"));
    assert!(r.junit().contains("&lt;bad&gt;"));
}

#[test]
fn parse_typed_graph_and_scalar_values() {
    use hydradb_cypher_tck_harness::values::{parse, Direction, Value};
    assert_eq!(parse("-42").unwrap(), Value::Integer(-42));
    assert_eq!(parse("-0.0").unwrap(), Value::Float((-0.0f64).to_bits()));
    assert_eq!(parse("1e2").unwrap(), Value::Float(100f64.to_bits()));
    assert!(matches!(parse("NaN").unwrap(), Value::Float(bits) if f64::from_bits(bits).is_nan()));
    assert!(matches!(parse("{a: [null, 'é\\\'x', (:L {x: 1})]}").unwrap(), Value::Map(_)));
    assert!(
        matches!(parse("<(:A)-[:R {x: 1}]->()<-[:S]-(:B)>").unwrap(), Value::Path(p) if p.segments.len() == 2 && p.segments[1].0 == Direction::Incoming)
    );
    assert!(parse("{a: 1, a: 2}").is_err());
    assert!(parse("[1,]").is_err());
}

#[tokio::test]
async fn coordinator_executes_once_and_cleans_up_on_failure_and_panic() {
    use hydradb_cypher_tck_harness::{
        lifecycle::*,
        steps::{Action, ExecutableStep, Kind},
    };
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    struct Fake {
        calls: Mutex<Vec<&'static str>>,
        panic_setup: bool,
        fail_setup: bool,
    }
    #[async_trait::async_trait]
    impl Adapter for Fake {
        type Session = ();
        async fn create(&self) -> Result<(), AdapterFailure> {
            self.calls.lock().unwrap().push("create");
            Ok(())
        }
        async fn fixture(&self, _: &mut (), _: &Kind) -> Result<(), AdapterFailure> {
            self.calls.lock().unwrap().push("fixture");
            Ok(())
        }
        async fn setup(&self, _: &mut (), _: &str) -> Result<(), AdapterFailure> {
            self.calls.lock().unwrap().push("setup");
            assert!(!self.panic_setup, "injected");
            if self.fail_setup {
                return Err(AdapterFailure::Setup("bad".into()));
            }
            Ok(())
        }
        async fn parameters(&self, _: &mut (), _: &BTreeMap<String, Value>) -> Result<(), AdapterFailure> {
            Ok(())
        }
        async fn syntax(&self, _: &mut (), _: &str) -> Result<(), Diagnostic> {
            self.calls.lock().unwrap().push("syntax");
            Ok(())
        }
        async fn prepare(&self, _: &mut (), _: &str) -> Result<(), Diagnostic> {
            self.calls.lock().unwrap().push("prepare");
            Ok(())
        }
        async fn execute(&self, _: &mut (), _: &str, line: usize) -> Result<QueryObservation, AdapterFailure> {
            self.calls.lock().unwrap().push("execute");
            Ok(QueryObservation {
                engine: "test".into(),
                execution_path: "test".into(),
                source_line: line,
                parser: "ok".into(),
                semantics: "ok".into(),
                outcome: QueryOutcome::Rows {
                    columns: vec![],
                    rows: vec![],
                },
            })
        }
        async fn observe_graph(&self, _: &mut ()) -> Result<GraphState, AdapterFailure> {
            Ok(GraphState::default())
        }
        async fn close(&self, _: ()) -> Result<(), AdapterFailure> {
            self.calls.lock().unwrap().push("close");
            Ok(())
        }
    }
    let steps = vec![
        ExecutableStep {
            line: 1,
            action: Action::Fixture(Kind::EmptyGraph),
        },
        ExecutableStep {
            line: 2,
            action: Action::Setup("setup".into()),
        },
        ExecutableStep {
            line: 3,
            action: Action::Execute {
                control: false,
                query: "query".into(),
            },
        },
        ExecutableStep {
            line: 4,
            action: Action::Empty,
        },
    ];
    let fake = Fake {
        calls: Mutex::new(vec![]),
        panic_setup: false,
        fail_setup: false,
    };
    assert!(run(&fake, &steps).await.unwrap().mismatches.is_empty());
    assert_eq!(
        *fake.calls.lock().unwrap(),
        vec!["create", "fixture", "setup", "syntax", "prepare", "execute", "close"]
    );
    for (panic_setup, fail_setup) in [(true, false), (false, true)] {
        let fake = Fake {
            calls: Mutex::new(vec![]),
            panic_setup,
            fail_setup,
        };
        let err = run(&fake, &steps).await.unwrap_err();
        assert!(matches!(err.failure, AdapterFailure::Infrastructure(_) | AdapterFailure::Setup(_)));
        assert_eq!(fake.calls.lock().unwrap().last(), Some(&"close"));
        assert!(!fake.calls.lock().unwrap().contains(&"execute"));
    }
}

#[test]
fn typed_binding_rejects_duplicate_parameters_and_invalid_effects() {
    use hydradb_cypher_tck_harness::steps::{bind_executable, bind_scenario};
    let feature = parse("Feature: F\n Scenario: S\n  Given an empty graph\n  And parameters are:\n   | x | 1 |\n  And parameters are:\n   | x | 2 |\n  When executing query:\n   \"\"\"\n   RETURN $x\n   \"\"\"\n  Then the result should be empty\n").unwrap();
    let case = expand(&feature, "local", "rev", "f").unwrap().remove(0);
    assert!(bind_scenario(&case).unwrap_err().contains("duplicate parameter"));
    let feature = parse("Feature: F\n Scenario: S\n  Then the side effects should be:\n   | +widgets | 1 |\n").unwrap();
    let case = expand(&feature, "local", "rev", "f").unwrap().remove(0);
    assert!(bind_executable(&case.steps[0]).is_err());
}

#[tokio::test]
async fn coordinator_matches_diagnostics_and_checks_error_side_effects() {
    use hydradb_cypher_tck_harness::{
        lifecycle::*,
        steps::{Action, ExecutableStep, Kind},
    };
    use std::collections::BTreeMap;
    struct Fake {
        wrong_detail: bool,
        mutate: bool,
    }
    #[async_trait::async_trait]
    impl Adapter for Fake {
        type Session = bool;
        async fn create(&self) -> Result<bool, AdapterFailure> {
            Ok(false)
        }
        async fn fixture(&self, _: &mut bool, _: &Kind) -> Result<(), AdapterFailure> {
            Ok(())
        }
        async fn setup(&self, _: &mut bool, _: &str) -> Result<(), AdapterFailure> {
            Ok(())
        }
        async fn parameters(&self, _: &mut bool, _: &BTreeMap<String, Value>) -> Result<(), AdapterFailure> {
            Ok(())
        }
        async fn syntax(&self, _: &mut bool, _: &str) -> Result<(), Diagnostic> {
            Ok(())
        }
        async fn prepare(&self, _: &mut bool, _: &str) -> Result<(), Diagnostic> {
            Ok(())
        }
        async fn execute(&self, s: &mut bool, _: &str, line: usize) -> Result<QueryObservation, AdapterFailure> {
            *s = self.mutate;
            Ok(QueryObservation {
                engine: "e".into(),
                execution_path: "p".into(),
                source_line: line,
                parser: "ok".into(),
                semantics: "ok".into(),
                outcome: QueryOutcome::Error(Diagnostic {
                    phase: "runtime".into(),
                    error_type: "TypeError".into(),
                    detail: if self.wrong_detail { "wrong" } else { "InvalidArgumentType" }.into(),
                    stage: "execution".into(),
                    native_code: None,
                    native_message: "native".into(),
                }),
            })
        }
        async fn observe_graph(&self, s: &mut bool) -> Result<GraphState, AdapterFailure> {
            let mut g = GraphState::default();
            if *s {
                g.nodes.insert("new".into());
            }
            Ok(g)
        }
        async fn close(&self, _: bool) -> Result<(), AdapterFailure> {
            Ok(())
        }
    }
    let steps = vec![
        ExecutableStep {
            line: 1,
            action: Action::Fixture(Kind::EmptyGraph),
        },
        ExecutableStep {
            line: 2,
            action: Action::Execute {
                control: false,
                query: "Q".into(),
            },
        },
        ExecutableStep {
            line: 3,
            action: Action::Error {
                phase: "runtime".into(),
                error_type: "TypeError".into(),
                detail: "InvalidArgumentType".into(),
            },
        },
    ];
    assert!(run(
        &Fake {
            wrong_detail: false,
            mutate: false
        },
        &steps
    )
    .await
    .unwrap()
    .mismatches
    .is_empty());
    assert_eq!(
        run(
            &Fake {
                wrong_detail: true,
                mutate: true
            },
            &steps
        )
        .await
        .unwrap()
        .mismatches
        .len(),
        2
    );
}
