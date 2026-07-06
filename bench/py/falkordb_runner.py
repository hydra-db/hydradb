"""FalkorDB bench runner — paired with turbolay-bench (and originally namidb-bench).

COPIED from graphdb-experiments/bench/falkordb_runner.py with this attribution
header and one addition: the `ic3h` 3-hop query (see cypher_for). The output
JSON shape is unchanged, so bench/py/compare.py aligns turbolay vs FalkorDB by
(query, param) without modification.


Loads the same LDBC CSV dataset into FalkorDB (must be running on
localhost:6379; `docker run -d -p 6379:6379 falkordb/falkordb:latest`)
and times IC02 / IC07 / IC08 / IC09 plus the synthetic IC4H 4-hop query.
Output JSON is shape-compatible with `namidb-bench run` and `kuzu_runner.py`.

Usage:
    # Standard bench (scale=1.0, 3 params, 50 warm runs):
    python3 bench/falkordb_runner.py --dataset-dir /tmp/snb-1.0 --warm-runs 50

    # High-degree hub bench (pass explicit person IDs):
    python3 bench/falkordb_runner.py --dataset-dir /tmp/snb-hub-320v2 \\
        --warm-runs 20 --only ic09 --only ic4h \\
        --top-degree-ids 50000000000000000000000000000000,...

Pre-requisites:
    docker run -d -p 6379:6379 falkordb/falkordb:latest
    pip install falkordb --break-system-packages
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

try:
    import falkordb
except ImportError:
    sys.stderr.write(
        "ERROR: falkordb not installed.\n"
        "  pip install falkordb --break-system-packages\n"
    )
    sys.exit(2)

# ── Constants ────────────────────────────────────────────────────────────

BATCH = 500  # rows per UNWIND query

PREFIX_TO_LABEL = {"50": "Person", "4f": "Post", "43": "Comment"}


def _label_of(hex_id: str) -> str:
    lbl = PREFIX_TO_LABEL.get(hex_id[:2].lower())
    if lbl is None:
        raise ValueError(f"unknown id prefix in {hex_id!r}")
    return lbl


def _percentile(samples: list[int], p: float) -> int:
    if not samples:
        return 0
    s = sorted(samples)
    return s[min(round((len(s) - 1) * p), len(s) - 1)]


def make_person_id_hex(i: int) -> str:
    """Match namidb_bench::main::make_person_id_hex."""
    out = bytearray(16)
    out[0] = ord("P")
    out[1:] = i.to_bytes(16, "big")[1:]
    return out.hex()


# ── Schema / indices ─────────────────────────────────────────────────────

def create_indices(g) -> None:
    """Create id indices on all three node labels for fast MATCH."""
    for label in ("Person", "Post", "Comment"):
        try:
            g.query(f"CREATE INDEX FOR (n:{label}) ON (n.id)")
        except Exception:
            pass  # already exists


# ── Node loaders ─────────────────────────────────────────────────────────

def _flush(g, cypher: str, batch: list) -> None:
    for i in range(0, len(batch), BATCH):
        g.query(cypher, {"rows": batch[i : i + BATCH]})


def load_persons(g, path: Path) -> None:
    batch: list = []
    with path.open() as f:
        next(f)
        for line in f:
            p = line.rstrip("\n").split("|")
            batch.append([p[0], p[1], p[2], int(p[3]), int(p[4])])
    _flush(
        g,
        "UNWIND $rows AS r CREATE (:Person "
        "{id:r[0],firstName:r[1],lastName:r[2],age:r[3],creationDate:r[4]})",
        batch,
    )


def load_posts(g, path: Path) -> None:
    batch: list = []
    with path.open() as f:
        next(f)
        for line in f:
            p = line.rstrip("\n").split("|")
            batch.append([p[0], p[1], int(p[2]), int(p[3])])
    _flush(
        g,
        "UNWIND $rows AS r CREATE (:Post "
        "{id:r[0],content:r[1],creationDate:r[2],length:r[3]})",
        batch,
    )


def load_comments(g, path: Path) -> None:
    batch: list = []
    with path.open() as f:
        next(f)
        for line in f:
            p = line.rstrip("\n").split("|")
            batch.append([p[0], p[1], int(p[2]), int(p[3])])
    _flush(
        g,
        "UNWIND $rows AS r CREATE (:Comment "
        "{id:r[0],content:r[1],creationDate:r[2],length:r[3]})",
        batch,
    )


# ── Edge loaders ─────────────────────────────────────────────────────────

def load_edges(
    g,
    path: Path,
    rel: str,
    prop_col: str | None = None,   # column name for the single extra prop (index 2)
) -> None:
    """Load edges, bucketing by (src_label, dst_label) for indexed MATCH.

    If prop_col is given the third CSV column is treated as an integer prop
    with that name (e.g. 'since' for KNOWS, 'creationDate' for LIKES).
    """
    buckets: dict[tuple[str, str], list] = defaultdict(list)
    with path.open() as f:
        next(f)
        for line in f:
            p = line.rstrip("\n").split("|")
            src_lbl = _label_of(p[0])
            dst_lbl = _label_of(p[1])
            if prop_col:
                buckets[(src_lbl, dst_lbl)].append([p[0], p[1], int(p[2])])
            else:
                buckets[(src_lbl, dst_lbl)].append([p[0], p[1]])

    for (src_lbl, dst_lbl), rows in buckets.items():
        if prop_col:
            cypher = (
                f"UNWIND $rows AS r "
                f"MATCH (a:{src_lbl} {{id:r[0]}}), (b:{dst_lbl} {{id:r[1]}}) "
                f"CREATE (a)-[:{rel} {{{prop_col}:r[2]}}]->(b)"
            )
        else:
            cypher = (
                f"UNWIND $rows AS r "
                f"MATCH (a:{src_lbl} {{id:r[0]}}), (b:{dst_lbl} {{id:r[1]}}) "
                f"CREATE (a)-[:{rel}]->(b)"
            )
        _flush(g, cypher, rows)


# ── Query Cypher ─────────────────────────────────────────────────────────

def cypher_for(query: str, person_id: str) -> str:
    pid = person_id
    if query == "ic02":
        return (
            f"MATCH (p:Person {{id: '{pid}'}})-[:KNOWS]->(friend:Person)"
            "<-[:HAS_CREATOR]-(message:Post) "
            "RETURN friend.firstName AS personFirstName, friend.lastName AS personLastName, "
            "message.content AS messageContent, message.creationDate AS messageCreationDate "
            "ORDER BY messageCreationDate DESC LIMIT 20"
        )
    if query == "ic07":
        return (
            f"MATCH (p:Person {{id: '{pid}'}})<-[:HAS_CREATOR]-(message:Post)"
            "<-[liker:LIKES]-(fan:Person) "
            "RETURN fan.firstName AS personFirstName, fan.lastName AS personLastName, "
            "liker.creationDate AS likeCreationDate, message.content AS messageContent "
            "ORDER BY likeCreationDate DESC LIMIT 20"
        )
    if query == "ic08":
        return (
            f"MATCH (p:Person {{id: '{pid}'}})<-[:HAS_CREATOR]-(post:Post)"
            "<-[:REPLY_OF]-(reply:Comment) "
            "RETURN reply.content AS replyContent, reply.creationDate AS replyDate, "
            "post.content AS postContent "
            "ORDER BY replyDate DESC LIMIT 20"
        )
    if query == "ic09":
        return (
            f"MATCH (p:Person {{id: '{pid}'}})-[:KNOWS]->(friend:Person)"
            "-[:KNOWS]->(fof:Person)<-[:HAS_CREATOR]-(msg:Post) "
            "RETURN fof.firstName AS personFirstName, fof.lastName AS personLastName, "
            "msg.content AS messageContent, msg.creationDate AS messageCreationDate "
            "ORDER BY messageCreationDate DESC LIMIT 20"
        )
    if query == "ic3h":
        return (
            f"MATCH (p:Person {{id: '{pid}'}})-[:KNOWS]->(f1:Person)"
            "-[:KNOWS]->(f2:Person)-[:KNOWS]->(f3:Person)"
            "<-[:HAS_CREATOR]-(msg:Post) "
            "RETURN f3.firstName AS personFirstName, f3.lastName AS personLastName, "
            "msg.content AS messageContent, msg.creationDate AS messageCreationDate "
            "ORDER BY messageCreationDate DESC LIMIT 20"
        )
    if query == "ic4h":
        return (
            f"MATCH (p:Person {{id: '{pid}'}})-[:KNOWS]->(f1:Person)"
            "-[:KNOWS]->(f2:Person)-[:KNOWS]->(f3:Person)"
            "-[:KNOWS]->(f4:Person)<-[:HAS_CREATOR]-(msg:Post) "
            "RETURN f4.firstName AS personFirstName, f4.lastName AS personLastName, "
            "msg.content AS messageContent, msg.creationDate AS messageCreationDate "
            "ORDER BY messageCreationDate DESC LIMIT 20"
        )
    raise ValueError(f"unknown query: {query!r}")


# ── Bench runner ─────────────────────────────────────────────────────────

def run_one(g, query: str, person_id: str, warm_runs: int) -> dict:
    cypher = cypher_for(query, person_id)

    t0 = time.perf_counter()
    rows = g.query(cypher).result_set
    cold_us = int((time.perf_counter() - t0) * 1_000_000)

    samples: list[int] = []
    for _ in range(warm_runs):
        t0 = time.perf_counter()
        g.query(cypher)
        samples.append(int((time.perf_counter() - t0) * 1_000_000))

    return {
        "backend": "falkordb",
        "query": query,
        "param": person_id,
        "rows": len(rows),
        "cold_us": cold_us,
        "warm_p50_us": _percentile(samples, 0.50),
        "warm_p95_us": _percentile(samples, 0.95),
        "warm_p99_us": _percentile(samples, 0.99),
        "warm_runs": warm_runs,
    }


# ── Loader ───────────────────────────────────────────────────────────────

def load_dataset(g, dataset_dir: Path) -> float:
    """Bulk-load all CSVs; return elapsed seconds."""
    t0 = time.perf_counter()
    create_indices(g)
    load_persons(g, dataset_dir / "persons.csv")
    load_posts(g, dataset_dir / "posts.csv")
    load_comments(g, dataset_dir / "comments.csv")
    load_edges(g, dataset_dir / "knows.csv",       "KNOWS",       prop_col="since")
    load_edges(g, dataset_dir / "has_creator.csv", "HAS_CREATOR", prop_col=None)
    load_edges(g, dataset_dir / "likes.csv",       "LIKES",       prop_col="creationDate")
    load_edges(g, dataset_dir / "reply_of.csv",    "REPLY_OF",    prop_col=None)
    return time.perf_counter() - t0


# ── CLI ──────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dataset-dir", required=True, type=Path)
    ap.add_argument("--warm-runs", type=int, default=50)
    ap.add_argument("--param-count", type=int, default=3)
    ap.add_argument(
        "--only", action="append", default=None,
        help="Limit to specific queries. Repeat for multiple.",
    )
    ap.add_argument(
        "--top-degree-ids", default=None,
        help="Comma-separated 32-hex person IDs to use as params (hub bench).",
    )
    ap.add_argument("--graph", default="ldbc_bench")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    client = falkordb.FalkorDB(host=args.host, port=args.port)

    # Wipe any previous run for reproducibility.
    try:
        client.select_graph(args.graph).delete()
    except Exception:
        pass
    g = client.select_graph(args.graph)

    print(f"loading {args.dataset_dir} into FalkorDB graph '{args.graph}' ...", file=sys.stderr)
    load_time = load_dataset(g, args.dataset_dir)
    n_elements = sum(
        g.query(f"MATCH (n:{lbl}) RETURN count(n)").result_set[0][0]
        for lbl in ("Person", "Post", "Comment")
    ) + sum(
        g.query(f"MATCH ()-[r:{rel}]->() RETURN count(r)").result_set[0][0]
        for rel in ("KNOWS", "HAS_CREATOR", "LIKES", "REPLY_OF")
    )
    print(
        f"loaded {n_elements:,} elements in {load_time:.2f}s "
        f"= {n_elements/load_time:,.0f} elem/s",
        file=sys.stderr,
    )

    queries = args.only or ["ic02", "ic07", "ic08", "ic09"]

    # Param selection: explicit ids or evenly-spaced.
    if args.top_degree_ids:
        params = [s.strip() for s in args.top_degree_ids.split(",")]
    else:
        n_persons = int(
            g.query("MATCH (p:Person) RETURN count(p)").result_set[0][0]
        )
        stride = max(1, n_persons // max(1, args.param_count))
        params = [make_person_id_hex(i * stride) for i in range(args.param_count)]

    results = []
    for q in queries:
        for param in params:
            r = run_one(g, q, param, args.warm_runs)
            print(
                f"  {r['query']} param={r['param'][:8]} rows={r['rows']} "
                f"cold={r['cold_us']}µs warm_p50={r['warm_p50_us']}µs",
                file=sys.stderr,
            )
            results.append(r)

    dataset_sizes = {
        lbl.lower() + "s": int(
            g.query(f"MATCH (n:{lbl}) RETURN count(n)").result_set[0][0]
        )
        for lbl in ("Person", "Post", "Comment")
    }
    dataset_sizes.update({
        rel.lower(): int(
            g.query(f"MATCH ()-[r:{rel}]->() RETURN count(r)").result_set[0][0]
        )
        for rel in ("KNOWS", "HAS_CREATOR", "LIKES", "REPLY_OF")
    })

    out = {
        "backend": "falkordb",
        "scale": args.scale,
        "seed": args.seed,
        "load_time_secs": round(load_time, 3),
        "elements": n_elements,
        "dataset_sizes": dataset_sizes,
        "results": results,
    }
    json.dump(out, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
