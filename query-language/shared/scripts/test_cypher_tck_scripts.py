"""Failure-injection tests for the process runner and aggregate policy gate."""
from __future__ import annotations

import copy
import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


aggregate_script = load_module("aggregate_cypher_tck", SCRIPT_DIR / "aggregate_cypher_tck.py")
runner_script = load_module("run_cypher_tck", SCRIPT_DIR / "run_cypher_tck.py")
ledger_script = load_module("validate_cypher_test_ledger", SCRIPT_DIR / "validate_cypher_test_ledger.py")


class LedgerValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(dir="/var/tmp")
        self.ledger = Path(self.temporary.name) / "coverage-migration.json"
        self.original_ledger = ledger_script.LEDGER
        self.ledger.write_text(ledger_script.LEDGER.read_text(encoding="utf-8"), encoding="utf-8")
        ledger_script.LEDGER = self.ledger

    def tearDown(self) -> None:
        ledger_script.LEDGER = self.original_ledger
        self.temporary.cleanup()

    def test_current_ledger_passes(self) -> None:
        self.assertEqual(ledger_script.validate(), (43, 455))

    def test_missing_byog_record_fails(self) -> None:
        payload = json.loads(self.ledger.read_text(encoding="utf-8"))
        removed = next(record for record in payload["records"] if record["target"] == "byog_identity")
        payload["records"].remove(removed)
        payload["record_count"] -= 1
        self.ledger.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "missing: byog_identity::"):
            ledger_script.validate()

    def test_stale_target_count_fails(self) -> None:
        payload = json.loads(self.ledger.read_text(encoding="utf-8"))
        payload["target_count"] -= 1
        self.ledger.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "target count mismatch"):
            ledger_script.validate()

    def test_stale_record_fails(self) -> None:
        payload = json.loads(self.ledger.read_text(encoding="utf-8"))
        stale = copy.deepcopy(next(record for record in payload["records"] if record["target"] == "statistics_access"))
        stale["key"] = "statistics_access::removed_ranking_test"
        stale["function"] = "removed_ranking_test"
        payload["records"].append(stale)
        payload["record_count"] += 1
        self.ledger.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "stale: statistics_access::removed_ranking_test"):
            ledger_script.validate()


class AggregateGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(dir="/var/tmp")
        self.root = Path(self.temporary.name)
        self.shards = self.root / "shards"
        self.output = self.root / "output"
        self.policy = self.root / "divergences.json"
        self.capabilities = self.root / "capabilities.json"
        self.case_id = "opencypher-tck@revision:feature.feature:1:1:-:-"
        self.inventory = {
            "engine": "legacy",
            "corpus_revision": "revision",
            "corpus_sha256": "sha256:digest",
            "feature_files": 220,
            "scenario_definitions": 1615,
            "expected_scenario_definitions": 1615,
            "expanded_cases": 1,
            "selected_cases": 1,
            "case_ids": [self.case_id],
            "execution_ids": [f"{self.case_id}#0"],
        }
        self.policy.write_text(json.dumps({"schema_version": 1, "divergences": []}), encoding="utf-8")
        self.capabilities.write_text(json.dumps({"schema_version": 1, "unsupported": []}), encoding="utf-8")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def records(self, engine: str) -> list[dict]:
        return [
            {
                "case_id": self.case_id,
                "engine": engine,
                "step": 0,
                "layer": layer,
                "verdict": "pass",
                "evidence": {},
            }
            for layer in sorted(aggregate_script.EXPECTED_LAYERS)
        ]

    def write_fixture(self, mutate=None) -> None:
        for engine in sorted(aggregate_script.EXPECTED_ENGINES):
            directory = self.shards / engine
            directory.mkdir(parents=True)
            inventory = copy.deepcopy(self.inventory)
            inventory["engine"] = engine
            records = self.records(engine)
            if mutate is not None:
                mutate(engine, inventory, records)
            (directory / "inventory.json").write_text(json.dumps(inventory), encoding="utf-8")
            (directory / "report.json").write_text(
                json.dumps({"corpus_revision": "revision", "records": records}), encoding="utf-8"
            )

    def aggregate(self) -> list[str]:
        return aggregate_script.aggregate(self.shards, self.output, self.policy, self.capabilities)

    def test_baseline_fixture_passes(self) -> None:
        self.write_fixture()
        self.assertEqual(self.aggregate(), [])

    def test_missing_report_record_fails(self) -> None:
        self.write_fixture(lambda engine, inventory, records: records.pop() if engine == "legacy" else None)
        self.assertTrue(any("incomplete four-level record" in error for error in self.aggregate()))

    def test_duplicate_inventory_identities_fail(self) -> None:
        def mutate(engine, inventory, records):
            inventory.update(
                expanded_cases=2,
                selected_cases=2,
                case_ids=[self.case_id, self.case_id],
                execution_ids=[f"{self.case_id}#0", f"{self.case_id}#0"],
            )

        self.write_fixture(mutate)
        errors = self.aggregate()
        self.assertTrue(any("case inventory contains duplicate identities" in error for error in errors))
        self.assertTrue(any("execution inventory contains duplicate identities" in error for error in errors))

    def test_execution_identity_must_reference_and_cover_inventory_cases(self) -> None:
        second = "opencypher-tck@revision:other.feature:1:1:-:-"

        def mutate(engine, inventory, records):
            inventory.update(
                expanded_cases=2,
                selected_cases=2,
                case_ids=[self.case_id, second],
                execution_ids=[f"{self.case_id}#0", "unknown#0"],
            )

        self.write_fixture(mutate)
        errors = self.aggregate()
        self.assertTrue(any("references unknown case" in error for error in errors))
        self.assertTrue(any("does not cover every case" in error for error in errors))

    def test_unknown_verdict_fails(self) -> None:
        self.write_fixture(
            lambda engine, inventory, records: records[0].update(verdict="unknown") if engine == "legacy" else None
        )
        self.assertTrue(any("unknown report verdict" in error for error in self.aggregate()))

    def test_adapter_failure_fails(self) -> None:
        def mutate(engine, inventory, records):
            if engine == "legacy":
                records[0]["evidence"]["adapter_failure"] = "process_timeout"
                records[0]["verdict"] = "blocked"

        self.write_fixture(mutate)
        self.assertTrue(any("adapter failure" in error and "process_timeout" in error for error in self.aggregate()))

    def test_cleanup_failure_fails(self) -> None:
        self.write_fixture(
            lambda engine, inventory, records: records[0]["evidence"].update(cleanup_failure="close failed")
            if engine == "legacy"
            else None
        )
        self.assertTrue(any("cleanup failure" in error for error in self.aggregate()))

    def test_skipped_verdict_fails(self) -> None:
        self.write_fixture(
            lambda engine, inventory, records: records[0].update(verdict="skipped") if engine == "legacy" else None
        )
        self.assertTrue(any("unexpected skipped verdict" in error for error in self.aggregate()))


class RunnerSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(dir="/var/tmp")
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_report_path_inside_repository_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "outside the repository"):
            runner_script.prepare_report_directory(self.root, self.root / "report")

    def test_process_failure_classifies_timeout_and_crash_separately(self) -> None:
        timeout_worker = self.root / "timeout-worker"
        timeout_worker.write_text("#!/bin/sh\nsleep 1\n", encoding="utf-8")
        timeout_worker.chmod(0o755)
        ok, timeout_reason = runner_script.run_process(timeout_worker, os.environ.copy(), self.root / "timeout.log", 0.01)
        self.assertFalse(ok)
        self.assertEqual(runner_script.process_failure_kind(timeout_reason), "process_timeout")

        crash_worker = self.root / "crash-worker"
        crash_worker.write_text("#!/bin/sh\nexit 23\n", encoding="utf-8")
        crash_worker.chmod(0o755)
        ok, crash_reason = runner_script.run_process(crash_worker, os.environ.copy(), self.root / "crash.log", 5)
        self.assertFalse(ok)
        self.assertEqual(crash_reason, "exit 23")
        self.assertEqual(runner_script.process_failure_kind(crash_reason), "process_exit")

    def test_unrelated_report_file_is_preserved(self) -> None:
        report = Path(tempfile.mkdtemp(dir="/var/tmp"))
        self.addCleanup(lambda: __import__("shutil").rmtree(report, ignore_errors=True))
        unrelated = report / "keep.txt"
        unrelated.write_text("keep", encoding="utf-8")
        (report / "report.json").write_text("owned", encoding="utf-8")
        runner_script.prepare_report_directory(self.root, report)
        self.assertEqual(unrelated.read_text(encoding="utf-8"), "keep")
        self.assertFalse((report / "report.json").exists())

    def test_failed_process_logs_survive_pruning(self) -> None:
        work = self.root / "work"
        shard = work / "shard"
        shard.mkdir(parents=True)
        (shard / "run.log").write_text("failure", encoding="utf-8")
        (work / "inventory.log").write_text("inventory", encoding="utf-8")
        (shard / "report.json").write_text("partial", encoding="utf-8")
        runner_script.prune_work_files(work)
        self.assertEqual((shard / "run.log").read_text(encoding="utf-8"), "failure")
        self.assertEqual((work / "inventory.log").read_text(encoding="utf-8"), "inventory")
        self.assertFalse((shard / "report.json").exists())


if __name__ == "__main__":
    unittest.main()
