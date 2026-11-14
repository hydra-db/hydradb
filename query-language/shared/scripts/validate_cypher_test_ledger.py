#!/usr/bin/env python3
"""Validate complete ownership of the 43 shared Cypher engine test targets."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "crates/cypher-engine/Cargo.toml"
LEDGER = ROOT / "query-language/shared/tests/coverage-migration.json"
TEST_ATTRIBUTE = re.compile(r"#\[(?:tokio::)?test(?:\([^]]*\))?\]\s*(?:#\[[^]]+\]\s*)*async\s+fn\s+([A-Za-z_][A-Za-z0-9_]*)|#\[(?:tokio::)?test(?:\([^]]*\))?\]\s*(?:#\[[^]]+\]\s*)*fn\s+([A-Za-z_][A-Za-z0-9_]*)", re.MULTILINE)
REQUIRED_FIELDS = {
    "key",
    "package",
    "target",
    "source_path",
    "function",
    "fixture",
    "dialect",
    "adapters",
    "required_layers",
    "assertion_contracts",
    "retained_bespoke",
    "provisional_replacements",
    "disposition",
}
ALLOWED_DISPOSITIONS = {"retained", "dual-covered", "migrated"}
ALLOWED_LAYERS = {"Parser", "Semantics", "Execution", "Diagnostics"}


def shared_targets() -> dict[str, Path]:
    manifest = tomllib.loads(MANIFEST.read_text(encoding="utf-8"))
    targets: dict[str, Path] = {}
    for target in manifest.get("test", []):
        path = (MANIFEST.parent / target["path"]).resolve()
        if "/query-language/shared/tests/engine/" not in path.as_posix():
            continue
        targets[target["name"]] = path
    return targets


def discovered_functions(targets: dict[str, Path]) -> set[str]:
    discovered: set[str] = set()
    for target, path in targets.items():
        source = path.read_text(encoding="utf-8")
        names = {left or right for left, right in TEST_ATTRIBUTE.findall(source)}
        if not names:
            raise ValueError(f"{target}: no test functions discovered in {path}")
        discovered.update(f"{target}::{name}" for name in names)
    return discovered


def validate() -> tuple[int, int]:
    payload = json.loads(LEDGER.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1:
        raise ValueError("ledger schema_version must be 1")
    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError("ledger records must be an array")

    targets = shared_targets()
    if payload.get("target_count") != len(targets) or len(targets) != 43:
        raise ValueError(f"target count mismatch: ledger={payload.get('target_count')} manifest={len(targets)}")
    if payload.get("record_count") != len(records):
        raise ValueError("record_count does not match records")

    keys: set[str] = set()
    covered_functions: set[str] = set()
    for index, record in enumerate(records):
        if not isinstance(record, dict) or set(record) != REQUIRED_FIELDS:
            raise ValueError(f"record {index} fields differ from the schema")
        key = record["key"]
        if key in keys:
            raise ValueError(f"duplicate ledger key: {key}")
        keys.add(key)
        target = record["target"]
        if target not in targets:
            raise ValueError(f"{key}: unknown target {target}")
        function_key = f"{target}::{record['function']}"
        if not key.startswith(function_key):
            raise ValueError(f"{key}: key must begin with {function_key}")
        covered_functions.add(function_key)
        expected_path = targets[target].relative_to(ROOT).as_posix()
        if record["source_path"] != expected_path:
            raise ValueError(f"{key}: source_path must be {expected_path}")
        if record["package"] != "hydradb-cypher-engine":
            raise ValueError(f"{key}: wrong package")
        if record["disposition"] not in ALLOWED_DISPOSITIONS:
            raise ValueError(f"{key}: invalid disposition")
        if not isinstance(record["required_layers"], list) or not set(record["required_layers"]).issubset(ALLOWED_LAYERS):
            raise ValueError(f"{key}: invalid required_layers")
        if not isinstance(record["adapters"], list) or not record["adapters"] or not all(isinstance(adapter, str) and adapter for adapter in record["adapters"]):
            raise ValueError(f"{key}: invalid adapters")
        for field in ("assertion_contracts", "retained_bespoke", "provisional_replacements"):
            if not isinstance(record[field], list):
                raise ValueError(f"{key}: {field} must be an array")
        if record["disposition"] == "migrated" and not record["provisional_replacements"]:
            raise ValueError(f"{key}: migrated record has no replacement")
        if record["disposition"] == "retained" and not record["retained_bespoke"]:
            raise ValueError(f"{key}: retained record needs a retained reason")

    discovered = discovered_functions(targets)
    missing = sorted(discovered - covered_functions)
    stale = sorted(covered_functions - discovered)
    if missing or stale:
        details = []
        if missing:
            details.append(f"missing: {', '.join(missing)}")
        if stale:
            details.append(f"stale: {', '.join(stale)}")
        raise ValueError("; ".join(details))
    return len(targets), len(discovered)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    targets, functions = validate()
    print(f"cypher-test-ledger-ok targets={targets} functions={functions}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
        print(f"cypher test ledger validation failed: {error}", file=sys.stderr)
        raise SystemExit(1)
