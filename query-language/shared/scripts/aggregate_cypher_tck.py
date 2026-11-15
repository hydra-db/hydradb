#!/usr/bin/env python3
"""Validate and aggregate deterministic openCypher TCK shard reports."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
POLICY = ROOT / "query-language" / "cypher" / "tck" / "divergences.json"
CAPABILITIES = ROOT / "query-language" / "cypher" / "tck" / "capabilities.json"
EXPECTED_ENGINES = {"legacy", "experimental"}
EXPECTED_LAYERS = {"parser", "semantics", "execution", "diagnostics"}
EXPECTED_VERDICTS = {"pass", "fail", "blocked", "not_applicable", "unsupported", "skipped"}
FAILURE_VERDICTS = {"fail"}


def load(path: Path):
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def signature(records: list[dict]) -> str:
    evidence = [
        {
            "step": record["step"],
            "verdict": record["verdict"],
            "evidence": record["evidence"],
        }
        for record in sorted(records, key=lambda record: record["step"])
        if record["verdict"] in FAILURE_VERDICTS
    ]
    return "sha256:" + hashlib.sha256(canonical(evidence).encode()).hexdigest()


def aggregate(
    shards: Path,
    output: Path,
    policy_path: Path = POLICY,
    capabilities_path: Path = CAPABILITIES,
) -> list[str]:
    output.mkdir(parents=True, exist_ok=True)
    errors: list[str] = []
    inventories = [load(path) for path in sorted(shards.rglob("inventory.json"))]
    reports = [load(path) for path in sorted(shards.rglob("report.json"))]
    engines = {inventory.get("engine") for inventory in inventories}
    execution_ids = inventories[0].get("execution_ids", []) if inventories else []
    if len(inventories) != 2 or engines != EXPECTED_ENGINES:
        errors.append(f"expected one inventory for each engine, got {sorted(str(engine) for engine in engines)}")
    if len(reports) != 2:
        errors.append(f"expected two reports, got {len(reports)}")

    if inventories:
        reference = inventories[0]
        shared = [
            "corpus_revision",
            "corpus_sha256",
            "feature_files",
            "scenario_definitions",
            "expected_scenario_definitions",
            "expanded_cases",
        ]
        for inventory in inventories:
            for field in shared:
                if inventory.get(field) != reference.get(field):
                    errors.append(f"inventory mismatch for {field}")
            if inventory.get("selected_cases") != inventory.get("expanded_cases"):
                errors.append(f"{inventory.get('engine')} did not execute the complete expanded inventory")
            if inventory.get("case_ids") != reference.get("case_ids"):
                errors.append(f"{inventory.get('engine')} case inventory differs")
            if inventory.get("execution_ids") != execution_ids:
                errors.append(f"{inventory.get('engine')} execution inventory differs")
            if inventory.get("feature_files") != 220 or inventory.get("scenario_definitions") != 1615:
                errors.append(f"{inventory.get('engine')} pinned corpus counts differ")
            case_ids = inventory.get("case_ids", [])
            inventory_execution_ids = inventory.get("execution_ids", [])
            if not isinstance(case_ids, list) or len(case_ids) != inventory.get("expanded_cases"):
                errors.append(f"{inventory.get('engine')} case inventory count differs from expanded cases")
            elif len(set(case_ids)) != len(case_ids):
                errors.append(f"{inventory.get('engine')} case inventory contains duplicate identities")
            if not isinstance(inventory_execution_ids, list) or len(set(inventory_execution_ids)) != len(inventory_execution_ids):
                errors.append(f"{inventory.get('engine')} execution inventory contains duplicate identities")
            parsed_execution_cases = set()
            for execution_id in inventory_execution_ids if isinstance(inventory_execution_ids, list) else []:
                try:
                    case_id, step = execution_id.rsplit("#", 1)
                    if int(step) < 0:
                        raise ValueError
                except (AttributeError, ValueError):
                    errors.append(f"{inventory.get('engine')} invalid execution identity {execution_id!r}")
                    continue
                if case_id not in case_ids:
                    errors.append(f"{inventory.get('engine')} execution identity references unknown case {case_id!r}")
                parsed_execution_cases.add(case_id)
            if isinstance(case_ids, list) and parsed_execution_cases != set(case_ids):
                errors.append(f"{inventory.get('engine')} execution inventory does not cover every case")

    records: list[dict] = []
    seen_records: set[tuple] = set()
    for report in reports:
        if inventories and report.get("corpus_revision") != inventories[0].get("corpus_revision"):
            errors.append("report corpus revision differs from inventory")
        for record in report.get("records", []):
            key = (record.get("case_id"), record.get("engine"), record.get("step"), record.get("layer"))
            if key in seen_records:
                errors.append(f"duplicate report record {key}")
            seen_records.add(key)
            if record.get("layer") not in EXPECTED_LAYERS:
                errors.append(f"unknown report layer in {key}")
            if record.get("verdict") not in EXPECTED_VERDICTS:
                errors.append(f"unknown report verdict in {key}: {record.get('verdict')!r}")
            records.append(record)

    grouped_steps: dict[tuple, set[str]] = defaultdict(set)
    for record in records:
        grouped_steps[(record["case_id"], record["engine"], record["step"])].add(record["layer"])
    expected_groups = {
        (execution_id.rsplit("#", 1)[0], engine, int(execution_id.rsplit("#", 1)[1]))
        for execution_id in execution_ids
        for engine in EXPECTED_ENGINES
    }
    for key in sorted(expected_groups - set(grouped_steps)):
        errors.append(f"missing execution group {key}")
    for key in sorted(set(grouped_steps) - expected_groups):
        errors.append(f"unexpected execution group {key}")
    for key, layers in grouped_steps.items():
        if layers != EXPECTED_LAYERS:
            errors.append(f"incomplete four-level record {key}: {sorted(layers)}")

    grouped_policy: dict[tuple, list[dict]] = defaultdict(list)
    for record in records:
        grouped_policy[(record["case_id"], record["engine"], record["layer"])].append(record)
    policy = load(policy_path)
    capabilities = load(capabilities_path)
    if policy.get("schema_version") != 1:
        errors.append("unsupported divergence policy schema")
    entries: dict[tuple, dict] = {}
    known_cases = set(inventories[0].get("case_ids", [])) if inventories else set()
    today = dt.date.today().isoformat()
    for entry in policy.get("divergences", []):
        key = (entry.get("case_id"), entry.get("engine"), entry.get("layer"))
        required = [
            "corpus_revision",
            "case_id",
            "engine",
            "layer",
            "expected_signature",
            "reason",
            "owner",
            "reference",
            "review_by",
        ]
        if any(not isinstance(entry.get(field), str) or not entry[field].strip() for field in required):
            errors.append(f"incomplete divergence {key}")
        if any("*" in str(entry.get(field, "")) for field in required):
            errors.append(f"wildcard divergence {key}")
        if key in entries:
            errors.append(f"duplicate divergence {key}")
        entries[key] = entry
        if inventories and entry.get("corpus_revision") != inventories[0].get("corpus_revision"):
            errors.append(f"wrong divergence corpus revision {key}")
        if entry.get("case_id") not in known_cases:
            errors.append(f"unknown divergence case {key}")
        try:
            if dt.date.fromisoformat(entry.get("review_by", "")) < dt.date.today():
                errors.append(f"expired divergence {key}")
        except ValueError:
            errors.append(f"invalid divergence review date {key}")

    for key, outcome_records in grouped_policy.items():
        failures = [record for record in outcome_records if record["verdict"] in FAILURE_VERDICTS]
        entry = entries.get(key)
        if failures:
            actual = signature(outcome_records)
            if entry is None:
                errors.append(f"unexpected failure {key} signature={actual}")
            elif entry.get("expected_signature") != actual:
                errors.append(f"divergence signature changed {key}: expected {entry.get('expected_signature')} actual {actual}")
        elif entry is not None:
            errors.append(f"unexpected pass {key}")
    for key in entries.keys() - grouped_policy.keys():
        errors.append(f"unobserved divergence {key}")

    unsupported_actual: dict[tuple[str, str], set[str]] = defaultdict(set)
    for record in records:
        if record["verdict"] == "unsupported":
            unsupported_actual[(record["case_id"], record["engine"])].add(
                str(record.get("evidence", {}).get("adapter_failure"))
            )
        adapter_failure = record.get("evidence", {}).get("adapter_failure")
        if adapter_failure is not None and record["verdict"] != "unsupported":
            errors.append(f"adapter failure {(record['case_id'], record['engine'], record['step'], record['layer'])}: {adapter_failure}")
        if record.get("evidence", {}).get("cleanup_failure") is not None:
            errors.append(
                f"cleanup failure {(record['case_id'], record['engine'], record['step'], record['layer'])}: "
                f"{record['evidence']['cleanup_failure']}"
            )
        if record["verdict"] == "skipped":
            errors.append(
                f"unexpected skipped verdict {(record['case_id'], record['engine'], record['step'], record['layer'])}"
            )

    if capabilities.get("schema_version") != 1:
        errors.append("unsupported capability policy schema")
    unsupported_expected: dict[tuple[str, str], str] = {}
    for entry in capabilities.get("unsupported", []):
        required = [
            "corpus_revision",
            "case_id",
            "capability",
            "observed_reason",
            "reason",
            "owner",
            "reference",
            "review_by",
        ]
        base = (entry.get("case_id"), entry.get("capability"))
        if any(not isinstance(entry.get(field), str) or not entry[field].strip() for field in required):
            errors.append(f"incomplete capability entry {base}")
        if any("*" in str(entry.get(field, "")) for field in required):
            errors.append(f"wildcard capability entry {base}")
        if entry.get("corpus_revision") != (inventories[0].get("corpus_revision") if inventories else None):
            errors.append(f"wrong capability corpus revision {base}")
        if entry.get("case_id") not in known_cases:
            errors.append(f"unknown capability case {base}")
        try:
            if dt.date.fromisoformat(entry.get("review_by", "")) < dt.date.today():
                errors.append(f"expired capability entry {base}")
        except ValueError:
            errors.append(f"invalid capability review date {base}")
        engines_value = entry.get("engines")
        if not isinstance(engines_value, list) or not engines_value or set(engines_value) - EXPECTED_ENGINES:
            errors.append(f"invalid capability engines {base}")
            continue
        for engine in engines_value:
            key = (entry.get("case_id"), engine)
            if key in unsupported_expected:
                errors.append(f"duplicate capability entry {key}")
            unsupported_expected[key] = entry.get("observed_reason")
    for key, actual_reasons in unsupported_actual.items():
        expected_reason = unsupported_expected.get(key)
        if expected_reason is None:
            errors.append(f"unexpected unsupported case {key}: {sorted(actual_reasons)}")
        elif actual_reasons != {expected_reason}:
            errors.append(
                f"unsupported capability changed {key}: expected {expected_reason!r}, actual {sorted(actual_reasons)}"
            )
    for key in unsupported_expected.keys() - unsupported_actual.keys():
        errors.append(f"unexpected capability support {key}")

    records.sort(key=lambda record: (record["case_id"], record["engine"], record["step"], record["layer"]))
    verdicts: dict[str, int] = defaultdict(int)
    for record in records:
        verdicts[record["verdict"]] += 1
    aggregate = {
        "schema_version": 1,
        "corpus_revision": inventories[0].get("corpus_revision") if inventories else None,
        "engines": sorted(engine for engine in engines if isinstance(engine, str)),
        "expanded_cases": inventories[0].get("expanded_cases") if inventories else 0,
        "verdicts": dict(sorted(verdicts.items())),
        "records": records,
        "policy_errors": errors,
    }
    (output / "aggregate.json").write_text(canonical(aggregate) + "\n", encoding="utf-8")
    lines = [
        f"openCypher TCK: {aggregate['expanded_cases']} expanded cases across {', '.join(aggregate['engines'])}",
        "verdicts: " + ", ".join(f"{key}={value}" for key, value in aggregate["verdicts"].items()),
        f"policy errors: {len(errors)}",
    ]
    lines.extend(f"- {error}" for error in errors)
    (output / "summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines[:3]))
    return errors


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: aggregate_cypher_tck.py SHARDS OUTPUT")
    errors = aggregate(Path(sys.argv[1]), Path(sys.argv[2]))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
