#!/usr/bin/env python3
"""Run the TCK integration test in hard-timeout process shards."""
from __future__ import annotations

import html
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

CHUNK = 100
CHUNK_TIMEOUT_SECONDS = 45
CASE_TIMEOUT_SECONDS = 5
TEST_NAME = "run_official_corpus_from_environment"
OWNED_REPORT_FILES = ("report.json", "report.jsonl", "junit.xml", "inventory.json", "summary.txt")


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compile_test(root: Path) -> Path:
    command = [
        "cargo", "test", "--locked", "--features", "opencypher,experimental-cypher-engine",
        "--test", "cypher_tck_conformance", "--no-run", "--message-format=json",
    ]
    completed = subprocess.run(command, cwd=root, text=True, stdout=subprocess.PIPE, check=True)
    executables = []
    for line in completed.stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("reason") == "compiler-artifact" and message.get("target", {}).get("name") == "cypher_tck_conformance":
            if message.get("executable"):
                executables.append(Path(message["executable"]))
    if len(executables) != 1:
        raise RuntimeError(f"expected one conformance test executable, got {executables}")
    return executables[0]


def run_process(executable: Path, env: dict[str, str], log: Path, timeout_seconds: int) -> tuple[bool, str]:
    with log.open("w", encoding="utf-8") as output:
        try:
            completed = subprocess.run(
                [str(executable), TEST_NAME, "--ignored", "--exact", "--nocapture"],
                env=env,
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=timeout_seconds,
            )
            return completed.returncode == 0, f"exit {completed.returncode}"
        except subprocess.TimeoutExpired:
            return False, f"hard timeout after {timeout_seconds}s"


def junit(records: list[dict]) -> str:
    failures = sum(record["verdict"] == "fail" for record in records)
    skipped = sum(record["verdict"] not in {"pass", "fail"} for record in records)
    lines = [f'<?xml version="1.0" encoding="UTF-8"?>', f'<testsuite tests="{len(records)}" failures="{failures}" skipped="{skipped}">']
    for record in records:
        name = html.escape(f"{record['case_id']}:{record['step']}:{record['layer']}", quote=True)
        engine = html.escape(record["engine"], quote=True)
        evidence = html.escape(canonical(record["evidence"]))
        if record["verdict"] == "fail":
            body = f"<failure>{evidence}</failure>"
        elif record["verdict"] == "pass":
            body = ""
        else:
            body = f'<skipped message="{html.escape(record["verdict"], quote=True)}">{evidence}</skipped>'
        lines.append(f'  <testcase classname="{engine}" name="{name}">{body}</testcase>')
    lines.append("</testsuite>")
    return "\n".join(lines) + "\n"


def prepare_report_directory(root: Path, report: Path) -> Path:
    root = root.resolve()
    report = report.resolve()
    if report == root or root in report.parents:
        raise ValueError(f"report directory must be outside the repository: {report}")
    report.mkdir(parents=True, exist_ok=True)
    for owned in OWNED_REPORT_FILES:
        (report / owned).unlink(missing_ok=True)
    work = report / ".shards"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    return work


def prune_work_files(work: Path) -> None:
    for path in work.rglob("*"):
        if path.is_file() and path.name not in {"run.log", "inventory.log"}:
            path.unlink()


def process_failure_kind(reason: str) -> str:
    return "process_timeout" if reason.startswith("hard timeout after ") else "process_exit"


def main() -> int:
    if len(sys.argv) != 4:
        raise SystemExit("usage: run_cypher_tck.py ENGINE FEATURES REPORT")
    engine, corpus_arg, report_arg = sys.argv[1:]
    if engine not in {"legacy", "experimental"}:
        raise SystemExit(f"unknown engine {engine}")
    root = Path(__file__).resolve().parents[3]
    corpus = Path(corpus_arg).resolve()
    report = Path(report_arg).resolve()
    try:
        work = prepare_report_directory(root, report)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    executable = compile_test(root)
    base_env = os.environ.copy()
    base_env.update({
        "TCK_ENGINE": engine,
        "TCK_CORPUS": str(corpus),
        "TCK_REPOSITORY_REVISION": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
    })

    inventory_dir = work / "inventory"
    inventory_dir.mkdir()
    inventory_env = base_env | {"TCK_REPORT": str(inventory_dir), "TCK_INVENTORY_ONLY": "1"}
    ok, reason = run_process(executable, inventory_env, work / "inventory.log", 60)
    if not ok:
        raise RuntimeError(f"inventory process failed: {reason}")
    inventory = json.loads((inventory_dir / "inventory.json").read_text(encoding="utf-8"))
    case_ids: list[str] = inventory["case_ids"]
    records: list[dict] = []
    completed_cases = 0

    def execute_range(start: int, count: int) -> None:
        nonlocal completed_cases
        shard = work / f"{start:05d}-{count:04d}"
        shard.mkdir(exist_ok=True)
        env = base_env | {
            "TCK_REPORT": str(shard),
            "TCK_CASE_START": str(start),
            "TCK_CASE_LIMIT": str(count),
        }
        timeout_seconds = CASE_TIMEOUT_SECONDS if count == 1 else CHUNK_TIMEOUT_SECONDS
        ok, reason = run_process(executable, env, shard / "run.log", timeout_seconds)
        report_path = shard / "report.json"
        if ok and report_path.is_file():
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            records.extend(payload["records"])
            completed_cases += count
            print(f"{engine}: isolated {completed_cases}/{len(case_ids)} expanded cases", flush=True)
            shutil.rmtree(shard)
            return
        if count > 1:
            midpoint = count // 2
            execute_range(start, midpoint)
            execute_range(start + midpoint, count - midpoint)
            return
        case_id = case_ids[start]
        evidence = {"adapter_failure": process_failure_kind(reason), "reason": reason}
        for layer in ("parser", "semantics", "execution", "diagnostics"):
            records.append({
                "case_id": case_id,
                "engine": engine,
                "step": 0,
                "layer": layer,
                "verdict": "blocked",
                "evidence": evidence,
            })
        completed_cases += 1
        print(f"{engine}: isolated {completed_cases}/{len(case_ids)} expanded cases; {case_id} {reason}", flush=True)

    for start in range(0, len(case_ids), CHUNK):
        execute_range(start, min(CHUNK, len(case_ids) - start))

    records.sort(key=lambda record: (record["case_id"], record["engine"], record["step"], record["layer"]))
    payload = {
        "schema_version": 1,
        "corpus_revision": inventory["corpus_revision"],
        "repository_revision": base_env["TCK_REPOSITORY_REVISION"],
        "records": records,
    }
    report.mkdir(parents=True, exist_ok=True)
    (report / "report.json").write_text(canonical(payload) + "\n", encoding="utf-8")
    (report / "report.jsonl").write_text("".join(canonical(record) + "\n" for record in records), encoding="utf-8")
    (report / "junit.xml").write_text(junit(records), encoding="utf-8")
    (report / "inventory.json").write_text(canonical(inventory) + "\n", encoding="utf-8")
    verdicts: dict[str, int] = {}
    for record in records:
        verdicts[record["verdict"]] = verdicts.get(record["verdict"], 0) + 1
    summary = f"{len(records)} verdicts: " + ", ".join(f"{key}={value}" for key, value in sorted(verdicts.items()))
    (report / "summary.txt").write_text(summary + "\n", encoding="utf-8")
    prune_work_files(work)
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
