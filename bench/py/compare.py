"""Side-by-side comparison of two bench result JSONs (turbolay vs FalkorDB).

COPIED VERBATIM (with this attribution header) from
graphdb-experiments/bench/compare.py. Aligns two result JSONs by
(query, param) and prints p50/p95 ratio tables. Works unchanged because both
runners emit the identical result-record shape.

Original module doc follows:


Reads two JSON files (NamiDB + Kuzu), aligns by (query, param) and
prints a table of warm_p50 / warm_p95 + ratio per row. Also emits an
aggregate section grouping by query.

Usage:
    python3 bench/compare.py tg.json kuzu.json
"""

from __future__ import annotations

import json
import sys
from statistics import median
from pathlib import Path


def fmt_us(v: int) -> str:
    if v >= 1_000_000:
        return f"{v/1_000_000:.2f}s"
    if v >= 1_000:
        return f"{v/1_000:.2f}ms"
    return f"{v}µs"


def load(path: Path) -> dict[tuple[str, str], dict]:
    blob = json.loads(path.read_text())
    out: dict[tuple[str, str], dict] = {}
    for r in blob["results"]:
        out[(r["query"], r["param"])] = r
    return out


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: compare.py <tg.json> <kuzu.json>", file=sys.stderr)
        return 2
    tg = load(Path(sys.argv[1]))
    kz = load(Path(sys.argv[2]))

    common = sorted(set(tg.keys()) & set(kz.keys()))
    if not common:
        print("no overlap between TG and Kuzu results", file=sys.stderr)
        return 1

    print(
        f"{'query':<6} {'param[:8]':<10} {'rows TG':>8} {'rows KZ':>8} "
        f"{'p50 TG':>10} {'p50 KZ':>10} {'p50 ratio':>10} "
        f"{'p95 TG':>10} {'p95 KZ':>10} {'p95 ratio':>10}"
    )
    print("-" * 110)

    per_query: dict[str, list[float]] = {}
    per_query_p95: dict[str, list[float]] = {}
    for q, p in common:
        t = tg[(q, p)]
        k = kz[(q, p)]
        p50_ratio = t["warm_p50_us"] / max(1, k["warm_p50_us"])
        p95_ratio = t["warm_p95_us"] / max(1, k["warm_p95_us"])
        print(
            f"{q:<6} {p[:8]:<10} {t['rows']:>8} {k['rows']:>8} "
            f"{fmt_us(t['warm_p50_us']):>10} {fmt_us(k['warm_p50_us']):>10} {p50_ratio:>9.2f}x "
            f"{fmt_us(t['warm_p95_us']):>10} {fmt_us(k['warm_p95_us']):>10} {p95_ratio:>9.2f}x"
        )
        per_query.setdefault(q, []).append(p50_ratio)
        per_query_p95.setdefault(q, []).append(p95_ratio)

    print()
    print("aggregate by query (median p50/p95 ratio across params):")
    print(f"{'query':<6} {'p50 ratio':>12} {'p95 ratio':>12} {'verdict':>20}")
    print("-" * 56)
    breach_2x = []
    for q in sorted(per_query):
        m50 = median(per_query[q])
        m95 = median(per_query_p95[q])
        verdict = "≤ 2x ✓" if max(m50, m95) <= 2.0 else "> 2x ✗"
        if max(m50, m95) > 2.0:
            breach_2x.append(q)
        print(f"{q:<6} {m50:>11.2f}x {m95:>11.2f}x {verdict:>20}")

    print()
    if breach_2x:
        print(f"GATE: FAIL — {len(breach_2x)} queries exceed 2× Kuzu ratio: {breach_2x}")
    else:
        print(f"GATE: PASS — all {len(per_query)} queries within 2× Kuzu (smoke scale=0.1)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
