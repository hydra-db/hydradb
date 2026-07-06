"""turbolay-vs-FalkorDB DISTINCT-result-set correctness oracle.

This is the *correctness* counterpart to `falkordb_runner.py` (which is a
*timing* comparison, `LIMIT 20`-truncated on both sides and therefore not a
row-for-row diff). Cypher's `MATCH` emits one row per distinct path, while
turbolay's BFS-over-postings dedups per hop (see `bench/src/queries.rs`'s
module doc) — so the only apples-to-apples comparison is the DISTINCT row
*set*, with no `LIMIT` on either side.

For each (query, param):
  1. Run `cargo run -p turbolay-bench -- verify` (or reuse an already-built
     binary) to get turbolay's `execute_distinct` row dump — no `LIMIT`,
     deduped by the full column tuple, JSON shape `{query, param, rows:
     [[col, ...], ...]}`.
  2. Run the *same* Cypher shape (`falkordb_runner.py::cypher_for`) against
     FalkorDB, but with `RETURN DISTINCT ...` and no `LIMIT`.
  3. Diff the two row sets (collapsed-to-set — order doesn't matter; we sort
     purely so a DIFF's offending rows print in a stable order).

Usage:
    python3 bench/py/verify_diff.py --dataset-dir /path/to/dataset --scale 0.1

Pre-requisites: same as falkordb_runner.py (FalkorDB running on
localhost:6379, `pip install falkordb`), plus a built `turbolay-bench`
binary (this script will `cargo build --release -p turbolay-bench` once and
reuse it if not told to skip that).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import falkordb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from falkordb_runner import cypher_for, load_dataset, make_person_id_hex  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]

ALL_QUERIES = ["ic02", "ic07", "ic08", "ic09", "ic3h", "ic4h"]


# ── Cypher: DISTINCT, no LIMIT ───────────────────────────────────────────

def cypher_for_distinct(query: str, person_id: str) -> str:
    """Same MATCH/RETURN shape as `cypher_for`, but `RETURN DISTINCT ...`
    and no `ORDER BY ... LIMIT 20` tail — the full correctness-oracle result
    set rather than the timed top-20 ranking.
    """
    base = cypher_for(query, person_id)
    # Every `cypher_for` arm's shape is `MATCH ... RETURN <cols> ORDER BY
    # <key> DESC LIMIT 20`; strip the ORDER BY/LIMIT tail and inject
    # DISTINCT right after RETURN. `MATCH`/`RETURN`/`ORDER BY` are fixed
    # literal separators `cypher_for` always emits, so this split is exact
    # (not a heuristic over arbitrary Cypher).
    order_by_idx = base.index(" ORDER BY ")
    head = base[:order_by_idx]
    assert " RETURN " in head, f"expected a RETURN clause in {head!r}"
    return head.replace(" RETURN ", " RETURN DISTINCT ", 1)


# ── Row normalization ─────────────────────────────────────────────────────

def _normalize_cell(v):
    """FalkorDB's client returns Python-native str/int/float; turbolay's
    JSON dump decodes to the same via `json.load`. Normalize numeric types
    (FalkorDB may return an int as a Python `int`, turbolay's `json!(i64)`
    also decodes to Python `int` via `json.load`) so tuple equality holds.
    """
    if isinstance(v, float) and v.is_integer():
        return int(v)
    return v


def _row_tuple(row) -> tuple:
    return tuple(_normalize_cell(c) for c in row)


# ── turbolay side ─────────────────────────────────────────────────────────

def build_turbolay_bench(release: bool) -> Path:
    cmd = ["cargo", "build", "-p", "turbolay-bench"]
    if release:
        cmd.append("--release")
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)
    profile = "release" if release else "debug"
    binary = REPO_ROOT / "target" / profile / "turbolay-bench"
    if not binary.exists():
        raise FileNotFoundError(f"expected built binary at {binary}")
    return binary


def run_turbolay_verify(
    binary: Path,
    dataset_dir: Path,
    scale: float,
    seed: int,
    param_count: int,
    queries: list[str],
) -> list[dict]:
    cmd = [
        str(binary),
        "verify",
        "--scale",
        str(scale),
        "--seed",
        str(seed),
        "--dataset-dir",
        str(dataset_dir),
        "--param-count",
        str(param_count),
    ]
    for q in queries:
        cmd += ["--only", q]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise RuntimeError(f"turbolay-bench verify failed (exit {proc.returncode})")
    return json.loads(proc.stdout)


# ── FalkorDB side ─────────────────────────────────────────────────────────

def falkordb_distinct_rows(g, query: str, person_id: str) -> list[tuple]:
    cypher = cypher_for_distinct(query, person_id)
    result = g.query(cypher).result_set
    return [_row_tuple(r) for r in result]


# ── Diffing ───────────────────────────────────────────────────────────────

def diff_one(query: str, param: str, turbolay_rows: list[tuple], falkor_rows: list[tuple]) -> dict:
    t_set = set(turbolay_rows)
    f_set = set(falkor_rows)

    # Sanity: neither side should have produced true row-content duplicates
    # once collapsed to DISTINCT (turbolay's own dedup is enforced in
    # `queries::finish_rows`; FalkorDB's dedup is `RETURN DISTINCT` itself).
    t_dupes = len(turbolay_rows) - len(t_set)
    f_dupes = len(falkor_rows) - len(f_set)

    only_turbolay = sorted(t_set - f_set)
    only_falkor = sorted(f_set - t_set)

    ok = not only_turbolay and not only_falkor
    return {
        "query": query,
        "param": param,
        "match": ok,
        "turbolay_count": len(t_set),
        "falkordb_count": len(f_set),
        "turbolay_raw_count": len(turbolay_rows),
        "falkordb_raw_count": len(falkor_rows),
        "turbolay_internal_dupes": t_dupes,
        "falkordb_internal_dupes": f_dupes,
        "only_in_turbolay": only_turbolay,
        "only_in_falkordb": only_falkor,
    }


# ── CLI ───────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dataset-dir", required=True, type=Path)
    ap.add_argument("--scale", type=float, default=0.1)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--param-count", type=int, default=3)
    ap.add_argument(
        "--only", action="append", default=None,
        help="Limit to specific queries. Repeat for multiple. Default: all six.",
    )
    ap.add_argument("--graph", default="ldbc_verify")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument(
        "--release", action="store_true",
        help="Build/run the release profile of turbolay-bench (faster at scale 1.0).",
    )
    ap.add_argument(
        "--max-print-diff-rows", type=int, default=10,
        help="Cap how many offending rows are printed per DIFF (0 = no cap).",
    )
    ap.add_argument(
        "--size-only", action="append", default=[],
        help=(
            "For these queries, only compare set *sizes* (not full row "
            "contents) — for deep-hop queries whose distinct set may be "
            "very large. Repeat for multiple."
        ),
    )
    args = ap.parse_args()

    queries = args.only or list(ALL_QUERIES)

    binary = build_turbolay_bench(args.release)

    print(f"loading {args.dataset_dir} into FalkorDB graph '{args.graph}' ...", file=sys.stderr)
    client = falkordb.FalkorDB(host=args.host, port=args.port)
    try:
        client.select_graph(args.graph).delete()
    except Exception:
        pass
    g = client.select_graph(args.graph)
    load_time = load_dataset(g, args.dataset_dir)
    print(f"loaded FalkorDB graph in {load_time:.2f}s", file=sys.stderr)

    n_persons = int(g.query("MATCH (p:Person) RETURN count(p)").result_set[0][0])
    stride = max(1, n_persons // max(1, args.param_count))
    params = [make_person_id_hex(i * stride) for i in range(args.param_count)]

    print(
        f"running turbolay verify (scale={args.scale} seed={args.seed} "
        f"param_count={args.param_count}) ...",
        file=sys.stderr,
    )
    turbolay_dump = run_turbolay_verify(
        binary, args.dataset_dir, args.scale, args.seed, args.param_count, queries
    )
    turbolay_by_key = {(d["query"], d["param"]): d["rows"] for d in turbolay_dump}

    reports = []
    all_ok = True
    for q in queries:
        for param in params:
            key = (q, param)
            t_rows_raw = turbolay_by_key.get(key)
            if t_rows_raw is None:
                print(f"  {q} param={param[:8]} -- MISSING from turbolay dump", file=sys.stderr)
                all_ok = False
                continue
            t_rows = [_row_tuple(r) for r in t_rows_raw]
            f_rows = falkordb_distinct_rows(g, q, param)

            if q in args.size_only:
                t_n, f_n = len(set(t_rows)), len(set(f_rows))
                ok = t_n == f_n
                all_ok &= ok
                status = "MATCH" if ok else "DIFF"
                print(
                    f"  {status} {q} param={param[:8]} (size-only) "
                    f"turbolay={t_n} falkordb={f_n}",
                    file=sys.stderr,
                )
                reports.append({
                    "query": q, "param": param, "match": ok, "size_only": True,
                    "turbolay_count": t_n, "falkordb_count": f_n,
                })
                continue

            report = diff_one(q, param, t_rows, f_rows)
            reports.append(report)
            all_ok &= report["match"]
            status = "MATCH" if report["match"] else "DIFF"
            print(
                f"  {status} {q} param={param[:8]} "
                f"turbolay={report['turbolay_count']} falkordb={report['falkordb_count']}",
                file=sys.stderr,
            )
            if not report["match"]:
                cap = args.max_print_diff_rows or None
                if report["only_in_turbolay"]:
                    rows = report["only_in_turbolay"][:cap]
                    print(f"    only in turbolay ({len(report['only_in_turbolay'])}): {rows}", file=sys.stderr)
                if report["only_in_falkordb"]:
                    rows = report["only_in_falkordb"][:cap]
                    print(f"    only in falkordb ({len(report['only_in_falkordb'])}): {rows}", file=sys.stderr)
                if report["turbolay_internal_dupes"] or report["falkordb_internal_dupes"]:
                    print(
                        f"    internal dupes: turbolay={report['turbolay_internal_dupes']} "
                        f"falkordb={report['falkordb_internal_dupes']}",
                        file=sys.stderr,
                    )

    print(json.dumps({"scale": args.scale, "seed": args.seed, "reports": reports}, indent=2))

    print(f"\n{'PASS' if all_ok else 'FAIL'}: verify_diff over {queries} @ scale={args.scale}", file=sys.stderr)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
