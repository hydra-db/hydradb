mod conformance_support;

use conformance_support::{experimental, legacy};
use hydradb_cypher_tck_harness::{
    lifecycle::{Adapter, QueryOutcome},
    steps::Kind,
    values::Value,
};
use std::collections::BTreeMap;

#[tokio::test]
async fn production_adapters_execute_reads_and_shared_transactional_writes() {
    let corpus = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("query-language/cypher/tck/upstream");
    for (engine, adapter) in [
        ("legacy", legacy::adapter(&corpus)),
        ("experimental", experimental::adapter(&corpus)),
    ] {
        let mut session = adapter.create().await.expect("create isolated graph");
        adapter.fixture(&mut session, &Kind::EmptyGraph).await.expect("empty fixture");
        adapter.parameters(&mut session, &BTreeMap::new()).await.expect("empty parameters");

        let create = "CREATE (a:A {id: 1})-[:LINK]->(b:B {id: 2})";
        adapter.syntax(&mut session, create).await.expect("create syntax");
        adapter.prepare(&mut session, create).await.expect("create preparation");
        let created = adapter.execute(&mut session, create, 1).await.expect("create execution");
        assert!(matches!(created.outcome, QueryOutcome::Rows { .. }), "{created:?}");

        let state = adapter.observe_graph(&mut session).await.expect("committed graph inspection");
        assert_eq!(state.nodes.len(), 2);
        assert_eq!(state.relationships.len(), 1);
        assert_eq!(state.labels.len(), 2);
        assert_eq!(
            state.properties.len(),
            0,
            "the production mutation path treats id as the graph identity"
        );

        let query = "MATCH (n:A) RETURN n.id AS id";
        adapter.syntax(&mut session, query).await.expect("read syntax");
        adapter.prepare(&mut session, query).await.expect("read preparation");
        let result = adapter.execute(&mut session, query, 2).await.expect("read execution");
        match result.outcome {
            QueryOutcome::Rows { columns, rows } => {
                assert_eq!(columns, ["id"]);
                assert_eq!(rows, vec![vec![Value::Integer(1)]]);
            }
            QueryOutcome::Error(error) => panic!("read returned diagnostic: {error:?}"),
        }

        if engine == "experimental" {
            macro_rules! execute_rows {
                ($query:expr) => {{
                    adapter.syntax(&mut session, $query).await.expect("graph-value syntax");
                    adapter.prepare(&mut session, $query).await.expect("graph-value preparation");
                    match adapter
                        .execute(&mut session, $query, 3)
                        .await
                        .expect("graph-value execution")
                        .outcome
                    {
                        QueryOutcome::Rows { columns, rows } => (columns, rows),
                        QueryOutcome::Error(error) => panic!("graph-value read returned diagnostic: {error:?}"),
                    }
                }};
            }
            let whole_node = "MATCH (n:A) RETURN n AS node";
            let (columns, rows) = execute_rows!(whole_node);
            assert_eq!(columns, ["node"]);
            assert!(matches!(rows.as_slice(), [row] if matches!(row.as_slice(), [Value::Node(_)])));

            let (columns, rows) = execute_rows!("MATCH (n) RETURN min(n.id) AS id");
            assert_eq!(columns, ["id"]);
            assert_eq!(rows, vec![vec![Value::Integer(1)]]);

            let (columns, rows) = execute_rows!("MATCH (n:A) WITH n RETURN n AS node");
            assert_eq!(columns, ["node"]);
            assert!(matches!(rows.as_slice(), [row] if matches!(row.as_slice(), [Value::Node(_)])));

            let (columns, rows) = execute_rows!("MATCH (n:A) RETURN *");
            assert_eq!(columns, ["n"]);
            assert!(matches!(rows.as_slice(), [row] if matches!(row.as_slice(), [Value::Node(_)])));

            let (columns, rows) = execute_rows!("MATCH (n:A) RETURN [n, n.id] AS values");
            assert_eq!(columns, ["values"]);
            assert!(matches!(
                rows.as_slice(),
                [row] if matches!(row.as_slice(), [Value::List(values)] if matches!(values.as_slice(), [Value::Node(_), Value::Integer(1)]))
            ));

            let (columns, rows) = execute_rows!("MATCH (n:A) RETURN {node: n, id: n.id} AS value");
            assert_eq!(columns, ["value"]);
            assert!(
                matches!(rows.as_slice(), [row] if matches!(row.as_slice(), [Value::Map(values)] if matches!(values.get("node"), Some(Value::Node(_))) && values.get("id") == Some(&Value::Integer(1))))
            );

            let (columns, rows) = execute_rows!("MATCH (n:A) RETURN collect(n) AS nodes");
            assert_eq!(columns, ["nodes"]);
            assert!(matches!(
                rows.as_slice(),
                [row] if matches!(row.as_slice(), [Value::List(values)] if matches!(values.as_slice(), [Value::Node(_)]))
            ));

            let (columns, rows) = execute_rows!("MATCH (n:A) WITH {node: n} AS m RETURN m.node AS node");
            assert_eq!(columns, ["node"]);
            assert!(matches!(rows.as_slice(), [row] if matches!(row.as_slice(), [Value::Node(_)])));

            for query in ["MATCH (n:A) LET x = n RETURN x", "MATCH (n:A) UNWIND [n] AS x RETURN x"] {
                let (columns, rows) = execute_rows!(query);
                assert_eq!(columns, ["x"]);
                assert!(
                    matches!(rows.as_slice(), [row] if matches!(row.as_slice(), [Value::Node(_)])),
                    "{query}: {rows:?}"
                );
            }
            let ambiguous = "MATCH (n:A) UNWIND [n, n.id] AS x RETURN x";
            adapter.syntax(&mut session, ambiguous).await.expect("ambiguous syntax");
            adapter.prepare(&mut session, ambiguous).await.expect("ambiguous preparation");
            let error = adapter
                .execute(&mut session, ambiguous, 3)
                .await
                .expect_err("ambiguous value must be unsupported");
            assert!(
                matches!(error, hydradb_cypher_tck_harness::lifecycle::AdapterFailure::Unsupported(message) if message.contains("ambiguous scalar-or-node provenance"))
            );
        }

        let delete = "MATCH (a:A)-[r:LINK]->(b:B) DELETE r RETURN count(r) AS deleted";
        adapter.syntax(&mut session, delete).await.expect("mutation return syntax");
        adapter.prepare(&mut session, delete).await.expect("mutation return preparation");
        let deleted = adapter.execute(&mut session, delete, 4).await.expect("mutation return execution");
        match deleted.outcome {
            QueryOutcome::Rows { columns, rows } => {
                assert_eq!(columns, ["deleted"]);
                assert_eq!(rows.len(), 1);
            }
            QueryOutcome::Error(error) => panic!("mutation returned diagnostic: {error:?}"),
        }
        adapter.close(session).await.expect("close graph");
    }
}

#[tokio::test]
async fn named_graph_fixture_resolves_extensionless_pinned_scripts() {
    let corpus = tempfile::tempdir().expect("create corpus directory");
    let graph = corpus.path().join("graphs/sample");
    std::fs::create_dir_all(&graph).expect("create named graph directory");
    std::fs::write(graph.join("sample.json"), r#"{"name":"sample","scripts":["sample"]}"#).expect("write named graph metadata");
    std::fs::write(graph.join("sample.cypher"), "CREATE (a:A {id: 1})-[:LINK]->(b:B {id: 2});").expect("write named graph script");
    for adapter in [legacy::adapter(corpus.path()), experimental::adapter(corpus.path())] {
        let mut session = adapter.create().await.expect("create isolated graph");
        adapter
            .fixture(&mut session, &Kind::NamedGraph("sample".into()))
            .await
            .expect("load named graph");
        let state = adapter.observe_graph(&mut session).await.expect("inspect named graph");
        assert_eq!(state.nodes.len(), 2);
        assert_eq!(state.relationships.len(), 1);
        adapter.close(session).await.expect("close graph");
    }
}

#[test]
fn local_declarative_cases_run_through_both_production_adapters() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let corpus = repository.join("query-language/cypher/cases");
    let upstream = repository.join("query-language/cypher/tck/upstream");
    let (defined, cases) = import_corpus(&corpus, "hydradb-local-v1", "hydradb-local");
    assert_eq!(defined, 3);
    assert_eq!(cases.len(), 3);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("create local-case runtime");
    for (engine, adapter) in [
        ("legacy", legacy::adapter(&upstream)),
        ("experimental", experimental::adapter(&upstream)),
    ] {
        for case in &cases {
            let id = case_id(&case.id);
            let bound = steps::bind_scenario(case).unwrap_or_else(|error| panic!("bind {id}: {error}"));
            let result = runtime.block_on(hydradb_cypher_tck_harness::lifecycle::run(&adapter, &bound));
            let records = records_for_case(&id, engine, &bound, result);
            assert!(local_records_pass(&records), "local case {id} failed on {engine}: {records:#?}");
        }
    }
}

fn local_records_pass(records: &[Record]) -> bool {
    records.iter().all(|record| {
        matches!(record.verdict, Verdict::Pass | Verdict::NotApplicable)
            && record.evidence.get("adapter_failure").is_none_or(serde_json::Value::is_null)
            && record.evidence.get("cleanup_failure").is_none_or(serde_json::Value::is_null)
    })
}

#[test]
fn local_gate_rejects_cleanup_failure_after_passing_execution() {
    let record = Record {
        case_id: "local-cleanup".into(),
        engine: "legacy".into(),
        step: 0,
        layer: "execution".into(),
        verdict: Verdict::Pass,
        evidence: json!({"adapter_failure": "Infrastructure(\"close failed\")", "cleanup_failure": null}),
    };
    assert!(!local_records_pass(&[record]));
}

use hydradb_cypher_tck_harness::{
    compare::{rows_equal, Order},
    importer::{self, CaseId, ExpandedCase},
    lifecycle::{effects, ExecutionRecord, RunFailure, RunResult},
    report::{Record, Report, Verdict},
    steps::{self, Action, ExecutableStep},
};
use serde::Deserialize;
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Deserialize)]
struct CorpusLock {
    commit: String,
    corpus_sha256: String,
    feature_files: usize,
    scenario_definitions: usize,
}

#[test]
#[ignore = "runs the complete pinned upstream corpus through one production adapter"]
fn run_official_corpus_from_environment() {
    let engine = std::env::var("TCK_ENGINE").expect("TCK_ENGINE is required");
    let corpus = PathBuf::from(std::env::var("TCK_CORPUS").expect("TCK_CORPUS is required"));
    let output = PathBuf::from(std::env::var("TCK_REPORT").expect("TCK_REPORT is required"));
    let repository_revision = std::env::var("TCK_REPOSITORY_REVISION").expect("TCK_REPOSITORY_REVISION is required");
    let start = std::env::var("TCK_CASE_START")
        .ok()
        .map(|value| value.parse::<usize>().expect("numeric TCK_CASE_START"))
        .unwrap_or(0);
    let limit = std::env::var("TCK_CASE_LIMIT")
        .ok()
        .map(|value| value.parse::<usize>().expect("numeric TCK_CASE_LIMIT"));
    let lock_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("query-language/cypher/tck/upstream.lock.json");
    let lock: CorpusLock = serde_json::from_slice(&fs::read(&lock_path).expect("read corpus lock")).expect("parse corpus lock");
    let (defined, all_cases) = import_corpus(&corpus, &lock.commit, "opencypher-tck");
    let expanded = all_cases.len();
    let cases = all_cases
        .into_iter()
        .skip(start)
        .take(limit.unwrap_or(usize::MAX))
        .collect::<Vec<_>>();
    fs::create_dir_all(&output).expect("create report directory");
    if std::env::var_os("TCK_INVENTORY_ONLY").is_some() {
        write_inventory(&output, &engine, &lock, defined, expanded, &cases);
        return;
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("create TCK runtime");
    let adapter = match engine.as_str() {
        "legacy" => legacy::adapter(corpus.parent().expect("features has tck parent")),
        "experimental" => experimental::adapter(corpus.parent().expect("features has tck parent")),
        other => panic!("unsupported TCK_ENGINE {other}"),
    };
    let mut records = Vec::new();
    for (index, case) in cases.iter().enumerate() {
        let case_id = case_id(&case.id);
        let bound = steps::bind_scenario(case).unwrap_or_else(|error| panic!("bind {case_id}: {error}"));
        let result = runtime.block_on(hydradb_cypher_tck_harness::lifecycle::run(&adapter, &bound));
        records.extend(records_for_case(&case_id, &engine, &bound, result));
        if (index + 1) % 100 == 0 || index + 1 == cases.len() {
            eprintln!("{engine}: executed {}/{} expanded cases", index + 1, cases.len());
        }
    }
    let report = Report {
        schema_version: 1,
        corpus_revision: lock.commit.clone(),
        repository_revision,
        records,
    };
    fs::write(
        output.join("report.json"),
        format!("{}\n", report.json().expect("serialize JSON report")),
    )
    .expect("write JSON report");
    fs::write(output.join("report.jsonl"), report.jsonl().expect("serialize JSONL report")).expect("write JSONL report");
    fs::write(output.join("junit.xml"), report.junit()).expect("write JUnit report");
    fs::write(output.join("summary.txt"), format!("{}\n", report.summary())).expect("write summary");
    write_inventory(&output, &engine, &lock, defined, expanded, &cases);
    assert_eq!(defined, lock.scenario_definitions, "scenario definition inventory changed");
}

fn write_inventory(output: &Path, engine: &str, lock: &CorpusLock, defined: usize, expanded: usize, cases: &[ExpandedCase]) {
    let case_ids = cases.iter().map(|case| case_id(&case.id)).collect::<Vec<_>>();
    let execution_ids = cases
        .iter()
        .flat_map(|case| {
            let id = case_id(&case.id);
            let bound = steps::bind_scenario(case).unwrap_or_else(|error| panic!("bind {id}: {error}"));
            let count = bound.iter().filter(|step| matches!(step.action, Action::Execute { .. })).count();
            (0..count).map(move |step| format!("{id}#{step}"))
        })
        .collect::<Vec<_>>();
    let inventory = json!({
        "schema_version": 1,
        "engine": engine,
        "corpus_revision": lock.commit.clone(),
        "corpus_sha256": lock.corpus_sha256.clone(),
        "feature_files": lock.feature_files,
        "scenario_definitions": defined,
        "expected_scenario_definitions": lock.scenario_definitions,
        "expanded_cases": expanded,
        "selected_cases": case_ids.len(),
        "case_ids": case_ids,
        "execution_ids": execution_ids,
    });
    fs::write(
        output.join("inventory.json"),
        format!("{}\n", serde_json::to_string(&inventory).expect("serialize inventory")),
    )
    .expect("write inventory");
}

fn import_corpus(root: &Path, revision: &str, namespace: &str) -> (usize, Vec<ExpandedCase>) {
    let mut paths = fs::read_dir(root)
        .expect("read corpus directory")
        .flat_map(|entry| collect_features(entry.expect("read corpus entry").path()))
        .collect::<Vec<_>>();
    paths.sort();
    let mut defined = 0;
    let mut cases = Vec::new();
    for path in paths {
        let relative = path
            .strip_prefix(root)
            .expect("feature under corpus")
            .to_string_lossy()
            .replace('\\', "/");
        let source = fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        let feature = importer::parse(&source).unwrap_or_else(|error| panic!("parse {relative}: {error}"));
        defined += feature.scenarios.len();
        cases.extend(
            importer::expand(&feature, namespace, revision, &relative).unwrap_or_else(|error| panic!("expand {relative}: {error}")),
        );
    }
    cases.sort_by(|left, right| left.id.cmp(&right.id));
    (defined, cases)
}

fn collect_features(path: PathBuf) -> Vec<PathBuf> {
    if path.is_file() {
        return (path.extension().and_then(|value| value.to_str()) == Some("feature"))
            .then_some(path)
            .into_iter()
            .collect();
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display())) {
        files.extend(collect_features(entry.expect("read nested corpus entry").path()));
    }
    files
}

fn case_id(id: &CaseId) -> String {
    format!(
        "{}@{}:{}:{}:{}:{}:{}",
        id.namespace,
        id.revision,
        id.path,
        id.scenario_line,
        id.scenario_column,
        id.example_line.map_or_else(|| "-".into(), |value| value.to_string()),
        id.example_row.map_or_else(|| "-".into(), |value| value.to_string()),
    )
}

fn records_for_case(case_id: &str, engine: &str, steps: &[ExecutableStep], result: Result<RunResult, RunFailure>) -> Vec<Record> {
    let (run, failure) = match result {
        Ok(run) => (run, None),
        Err(failure) => (failure.partial.clone(), Some(failure)),
    };
    let executions = steps
        .iter()
        .filter(|step| matches!(step.action, Action::Execute { .. }))
        .collect::<Vec<_>>();
    let mut records = Vec::new();
    for (index, execution_step) in executions.iter().enumerate() {
        let expectation = expectation_after(steps, execution_step.line);
        if let Some(execution) = run.executions.get(index) {
            records.extend(records_for_execution(
                case_id,
                engine,
                index,
                execution,
                expectation,
                &run.mismatches,
            ));
        } else {
            let verdict = match failure.as_ref().map(|failure| &failure.failure) {
                Some(hydradb_cypher_tck_harness::lifecycle::AdapterFailure::Unsupported(_)) => Verdict::Unsupported,
                _ => Verdict::Blocked,
            };
            let evidence = json!({"adapter_failure": failure.as_ref().map(|failure| format!("{:?}", failure.failure))});
            for layer in ["parser", "semantics", "execution", "diagnostics"] {
                records.push(Record {
                    case_id: case_id.into(),
                    engine: engine.into(),
                    step: index,
                    layer: layer.into(),
                    verdict: verdict.clone(),
                    evidence: evidence.clone(),
                });
            }
        }
    }
    if let Some(run_failure) = failure {
        let adapter_failure = format!("{:?}", run_failure.failure);
        let cleanup_failure = run_failure.cleanup.map(|failure| format!("{failure:?}"));
        for record in &mut records {
            if record.step + 1 == executions.len() {
                let evidence = record.evidence.as_object_mut().expect("record evidence is an object");
                evidence.insert("adapter_failure".into(), json!(adapter_failure));
                evidence.insert("cleanup_failure".into(), json!(cleanup_failure));
            }
        }
    }
    records
}

fn expectation_after(steps: &[ExecutableStep], execute_line: usize) -> Vec<&Action> {
    let start = steps
        .iter()
        .position(|step| step.line == execute_line && matches!(step.action, Action::Execute { .. }))
        .expect("execution step exists");
    steps[start + 1..]
        .iter()
        .take_while(|step| !matches!(step.action, Action::Execute { .. }))
        .map(|step| &step.action)
        .collect()
}

fn records_for_execution(
    case_id: &str,
    engine: &str,
    step: usize,
    execution: &ExecutionRecord,
    expectations: Vec<&Action>,
    mismatches: &[String],
) -> Vec<Record> {
    let expected_error = expectations.iter().find_map(|action| match action {
        Action::Error { error_type, phase, detail } => Some((error_type.as_str(), phase.as_str(), detail.as_str())),
        _ => None,
    });
    let diagnostic_matches = expected_error
        .zip(execution.diagnostic())
        .is_some_and(|((error_type, phase, detail), actual)| {
            (phase == "any time" || phase == actual.phase) && error_type == actual.error_type && (detail == "*" || detail == actual.detail)
        });
    let actual_effects = effects(&execution.before, &execution.after);
    let positive_matches = assertions_match(execution, &expectations, &actual_effects);
    let evidence = json!({
        "source_line": execution.source_line,
        "syntax": execution.syntax,
        "preparation": execution.preparation,
        "observation": execution.observation,
        "expected_error": expected_error,
        "effects": actual_effects,
        "mismatches": mismatches,
    });
    let parser = if execution.syntax.is_ok() || diagnostic_matches {
        Verdict::Pass
    } else {
        Verdict::Fail
    };
    let semantics = match &execution.preparation {
        Some(Ok(())) => Verdict::Pass,
        Some(Err(_)) if diagnostic_matches => Verdict::Pass,
        Some(Err(_)) => Verdict::Fail,
        None if diagnostic_matches => Verdict::NotApplicable,
        None => Verdict::Blocked,
    };
    let error_has_no_effects = actual_effects == Default::default();
    let execution_verdict = match (&execution.observation, expected_error) {
        (Some(_), Some(_)) if diagnostic_matches && positive_matches && error_has_no_effects => Verdict::Pass,
        (Some(_), Some(_)) => Verdict::Fail,
        (Some(_), None) if positive_matches => Verdict::Pass,
        (Some(_), None) => Verdict::Fail,
        (None, Some((_, "compile time", _))) if diagnostic_matches => Verdict::NotApplicable,
        (None, Some((_, "any time", _))) if diagnostic_matches => Verdict::NotApplicable,
        (None, _) => Verdict::Blocked,
    };
    let diagnostics = match expected_error {
        None => Verdict::NotApplicable,
        Some(_) if diagnostic_matches => Verdict::Pass,
        Some(_) => Verdict::Fail,
    };
    [
        ("parser", parser),
        ("semantics", semantics),
        ("execution", execution_verdict),
        ("diagnostics", diagnostics),
    ]
    .into_iter()
    .map(|(layer, verdict)| Record {
        case_id: case_id.into(),
        engine: engine.into(),
        step,
        layer: layer.into(),
        verdict,
        evidence: evidence.clone(),
    })
    .collect()
}

fn assertions_match(
    execution: &ExecutionRecord,
    expectations: &[&Action],
    actual_effects: &hydradb_cypher_tck_harness::lifecycle::Effects,
) -> bool {
    expectations.iter().all(|action| match action {
        Action::Rows { columns, rows, ordered, list_order_ignored } => match &execution.observation {
            Some(observation) => match &observation.outcome {
                QueryOutcome::Rows { columns: actual_columns, rows: actual_rows } => rows_equal(
                    columns,
                    rows,
                    actual_columns,
                    actual_rows,
                    if *ordered { Order::Ordered } else { Order::Unordered },
                    if *list_order_ignored { Order::Unordered } else { Order::Ordered },
                ),
                _ => false,
            },
            None => false,
        },
        Action::Empty => matches!(&execution.observation, Some(observation) if matches!(&observation.outcome, QueryOutcome::Rows { rows, .. } if rows.is_empty())),
        Action::Effects(expected) => actual_effects == expected,
        Action::Error { .. } => true,
        _ => true,
    })
}

#[test]
fn signed_evidence_contains_exact_graph_effects_and_changes_with_counts() {
    use hydradb_cypher_tck_harness::lifecycle::{Effects, GraphState, QueryObservation};
    use sha2::{Digest, Sha256};
    use std::collections::{BTreeMap, BTreeSet};

    let before = GraphState {
        nodes: BTreeSet::from(["removed-node".into()]),
        relationships: BTreeSet::from(["removed-relationship".into()]),
        labels: BTreeSet::from([("removed-node".into(), "Old".into())]),
        properties: BTreeMap::from([(("removed-node".into(), "old".into()), Value::Integer(1))]),
    };
    let after = GraphState {
        nodes: BTreeSet::from(["added-node".into()]),
        relationships: BTreeSet::from(["added-relationship".into()]),
        labels: BTreeSet::from([("added-node".into(), "New".into())]),
        properties: BTreeMap::from([(("added-node".into(), "new".into()), Value::Integer(2))]),
    };
    let execution = ExecutionRecord {
        source_line: 7,
        control: false,
        syntax: Ok(()),
        preparation: Some(Ok(())),
        observation: Some(QueryObservation {
            engine: "legacy".into(),
            execution_path: "test".into(),
            source_line: 7,
            parser: "accepted".into(),
            semantics: "prepared".into(),
            outcome: QueryOutcome::Rows {
                columns: Vec::new(),
                rows: Vec::new(),
            },
        }),
        before,
        after,
    };
    let expected = Action::Effects(Effects::default());
    let records = records_for_execution("effects", "legacy", 0, &execution, vec![&expected], &[]);
    let evidence = &records[0].evidence;
    assert_eq!(
        evidence["effects"],
        json!({
            "nodes_added": 1,
            "nodes_removed": 1,
            "relationships_added": 1,
            "relationships_removed": 1,
            "labels_added": 1,
            "labels_removed": 1,
            "properties_added": 1,
            "properties_removed": 1,
        })
    );

    let mut changed = execution.clone();
    changed.after.nodes.insert("second-added-node".into());
    let changed_records = records_for_execution("effects", "legacy", 0, &changed, vec![&expected], &[]);
    let signature = |records: &[Record]| {
        let failed = records
            .iter()
            .filter(|record| record.verdict == Verdict::Fail)
            .map(|record| json!({"step": record.step, "verdict": record.verdict, "evidence": record.evidence}))
            .collect::<Vec<_>>();
        format!(
            "sha256:{:x}",
            Sha256::digest(serde_json::to_vec(&failed).expect("serialize evidence"))
        )
    };
    assert_ne!(signature(&records), signature(&changed_records));
}
