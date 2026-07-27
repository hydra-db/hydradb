#!/usr/bin/env python3
"""Benchmark exact-hop GraphBLAS queries through the official Neo4j driver."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import neo4j
from neo4j import GraphDatabase


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def parse_hops(value: str) -> tuple[int, ...]:
    hops = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    if not hops or any(hop <= 0 or hop > 32 for hop in hops):
        raise argparse.ArgumentTypeError("hops must contain integers from 1 through 32")
    return hops


def cypher_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise argparse.ArgumentTypeError("must be a static Cypher identifier")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default="bolt://127.0.0.1:17687")
    parser.add_argument("--token", required=True)
    parser.add_argument("--database", default="default")
    parser.add_argument("--source-label", type=cypher_identifier)
    parser.add_argument("--degree", type=positive_int, default=30)
    parser.add_argument("--hops", type=parse_hops, default=(1, 3, 5, 10))
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--samples", type=positive_int, default=100)
    parser.add_argument("--concurrency", type=positive_int, default=8)
    parser.add_argument("--operations-per-worker", type=positive_int, default=50)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.warmup < 0:
        parser.error("--warmup must not be negative")
    return args


def query(hop: int, source_label: str | None = None) -> str:
    label = f":{source_label}" if source_label else ""
    return (
        f"MATCH (u{label} {{id: 1}})-[:BENCH*{hop}..{hop}]->(v) "
        "RETURN count(*) AS total"
    )


def execute_read(session, hop: int, expected: int, source_label: str | None = None) -> float:
    started = time.perf_counter_ns()
    row = session.run(query(hop, source_label)).single(strict=True)
    elapsed_us = (time.perf_counter_ns() - started) / 1_000.0
    if row["total"] != expected:
        raise AssertionError(
            f"hop {hop}: expected {expected} reachable vertices, got {row['total']}"
        )
    return elapsed_us


def nearest_rank(values: list[float], percentile: int) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(percentile / 100.0 * len(ordered)) - 1)
    return ordered[index]


def latency_record(hop: int, degree: int, samples: list[float]) -> dict:
    return {
        "kind": "latency",
        "degree": degree,
        "hops": hop,
        "samples": len(samples),
        "concurrency": 1,
        "p50_us": nearest_rank(samples, 50),
        "p95_us": nearest_rank(samples, 95),
        "p99_us": nearest_rank(samples, 99),
        "mean_us": statistics.fmean(samples),
        "qps": 0.0,
    }


def concurrent_reads(driver, args: argparse.Namespace, hop: int) -> tuple[list[float], float]:
    barrier = threading.Barrier(args.concurrency)

    def worker(_: int) -> list[float]:
        with driver.session(database=args.database) as session:
            barrier.wait(timeout=30)
            return [
                execute_read(session, hop, args.degree, args.source_label)
                for _ in range(args.operations_per_worker)
            ]

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        worker_samples = list(executor.map(worker, range(args.concurrency)))
    elapsed = time.perf_counter() - started
    return [sample for samples in worker_samples for sample in samples], elapsed


def throughput_record(
    hop: int, degree: int, concurrency: int, samples: list[float], elapsed: float
) -> dict:
    return {
        "kind": "throughput",
        "degree": degree,
        "hops": hop,
        "samples": len(samples),
        "concurrency": concurrency,
        "p50_us": nearest_rank(samples, 50),
        "p95_us": nearest_rank(samples, 95),
        "p99_us": nearest_rank(samples, 99),
        "mean_us": statistics.fmean(samples),
        "qps": len(samples) / elapsed,
    }


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    records = []
    query_calls = 0
    with GraphDatabase.driver(
        args.uri,
        auth=("neo4j", args.token),
        max_connection_pool_size=max(16, args.concurrency + 2),
    ) as driver:
        driver.verify_connectivity()
        for hop in args.hops:
            with driver.session(database=args.database) as session:
                for _ in range(args.warmup):
                    execute_read(session, hop, args.degree, args.source_label)
                query_calls += args.warmup
                samples = [
                    execute_read(session, hop, args.degree, args.source_label)
                    for _ in range(args.samples)
                ]
                query_calls += args.samples
            records.append(latency_record(hop, args.degree, samples))

            samples, elapsed = concurrent_reads(driver, args, hop)
            query_calls += len(samples)
            records.append(
                throughput_record(hop, args.degree, args.concurrency, samples, elapsed)
            )

    fields = [
        "kind",
        "degree",
        "hops",
        "samples",
        "concurrency",
        "p50_us",
        "p95_us",
        "p99_us",
        "mean_us",
        "qps",
    ]
    with (args.output_dir / "results.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)
    metadata = {
        "neo4j_driver_version": neo4j.__version__,
        "uri": args.uri,
        "database": args.database,
        "degree": args.degree,
        "hops": args.hops,
        "warmup": args.warmup,
        "latency_samples": args.samples,
        "concurrency": args.concurrency,
        "operations_per_worker": args.operations_per_worker,
        "query_calls": query_calls,
    }
    (args.output_dir / "client_metrics.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )

    print("kind  hop  p50_us  p95_us  p99_us  mean_us  qps")
    for record in records:
        print(
            f"{record['kind']:<10} {record['hops']:>3} "
            f"{record['p50_us']:>8.3f} {record['p95_us']:>8.3f} "
            f"{record['p99_us']:>8.3f} {record['mean_us']:>8.3f} "
            f"{record['qps']:>8.2f}"
        )
    print(f"query_calls={query_calls}")


if __name__ == "__main__":
    main()
