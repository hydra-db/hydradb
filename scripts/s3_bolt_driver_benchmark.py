#!/usr/bin/env python3
import argparse
import csv
import math
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from neo4j import GraphDatabase


HOPS = (1, 3, 5, 10)
READ_QUERY = (
    "MATCH (u {{id: 1}})-[:BENCH_H{hop}*{hop}..{hop}]->(v) "
    "RETURN count(*) AS total"
)
WRITE_QUERY = "CREATE (a {id: $src})-[:BENCH_WRITE]->(b {id: $dst})"
DELETE_QUERY = (
    "MATCH (a {id: $src})-[r:BENCH_WRITE]->(b {id: $dst}) DELETE r"
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("cold", "hot", "summarize"))
    parser.add_argument("--uri", default="bolt://127.0.0.1:17687")
    parser.add_argument("--token", default="s3-bolt-benchmark-secret-32-chars")
    parser.add_argument("--fanout", type=int)
    parser.add_argument("--hop", type=int, choices=HOPS)
    parser.add_argument("--latency-csv", required=True)
    parser.add_argument("--throughput-csv", required=True)
    parser.add_argument("--summary-csv")
    parser.add_argument("--hot-iters", type=int, default=40)
    parser.add_argument("--read-concurrency", type=int, default=8)
    parser.add_argument("--read-ops-per-worker", type=int, default=20)
    parser.add_argument("--mutation-iters", type=int, default=20)
    parser.add_argument("--mutation-concurrency", type=int, default=4)
    parser.add_argument("--mutation-ops-per-worker", type=int, default=10)
    parser.add_argument("--skip-mutations", action="store_true")
    return parser.parse_args()


def driver_for(args):
    driver = GraphDatabase.driver(
        args.uri,
        auth=("neo4j", args.token),
        max_connection_pool_size=256,
        connection_timeout=30,
    )
    driver.verify_connectivity()
    return driver


def timed_read(session, fanout, hop):
    query = READ_QUERY.format(hop=hop)
    started = time.perf_counter_ns()
    row = session.run(query).single(strict=True)
    elapsed = time.perf_counter_ns() - started
    if row["total"] != fanout:
        raise AssertionError(
            f"fanout={fanout} hop={hop}: expected {fanout}, got {row['total']}"
        )
    return elapsed / 1_000.0


def timed_mutation(session, query, src, dst):
    started = time.perf_counter_ns()
    session.run(query, src=src, dst=dst).consume()
    return (time.perf_counter_ns() - started) / 1_000.0


def append_rows(path, header, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        if not exists:
            writer.writerow(header)
        writer.writerows(rows)


def run_cold(args):
    if args.fanout is None:
        raise ValueError("--fanout is required")
    if args.hop is None:
        raise ValueError("--hop is required for a cold sample")
    rows = []
    with driver_for(args) as driver:
        with driver.session(database="default") as session:
            rows.append(
                (
                    "read",
                    args.fanout,
                    args.hop,
                    "cold",
                    timed_read(session, args.fanout, args.hop),
                )
            )
    append_rows(
        args.latency_csv,
        ("kind", "fanout", "hops", "path", "latency_us"),
        rows,
    )


def concurrent_operations(driver, workers, per_worker, operation):
    def worker(worker_id):
        samples = []
        with driver.session(database="default") as session:
            for index in range(per_worker):
                samples.append(operation(session, worker_id, index))
        return samples

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(worker, range(workers)))
    elapsed = time.perf_counter() - started
    samples = [sample for worker_samples in results for sample in worker_samples]
    return samples, elapsed


def run_hot(args):
    if args.fanout is None:
        raise ValueError("--fanout is required")
    latency_rows = []
    throughput_rows = []
    with driver_for(args) as driver:
        for hop in HOPS:
            with driver.session(database="default") as session:
                for _ in range(5):
                    timed_read(session, args.fanout, hop)
                for _ in range(args.hot_iters):
                    latency_rows.append(
                        ("read", args.fanout, hop, "hot", timed_read(session, args.fanout, hop))
                    )

            samples, elapsed = concurrent_operations(
                driver,
                args.read_concurrency,
                args.read_ops_per_worker,
                lambda session, _worker, _index: timed_read(
                    session, args.fanout, hop
                ),
            )
            latency_rows.extend(
                ("read_concurrent", args.fanout, hop, "hot", sample)
                for sample in samples
            )
            operations = len(samples)
            throughput_rows.append(
                (
                    "read",
                    args.fanout,
                    hop,
                    "hot",
                    args.read_concurrency,
                    operations,
                    elapsed,
                    operations / elapsed,
                )
            )

        if args.skip_mutations:
            append_rows(
                args.latency_csv,
                ("kind", "fanout", "hops", "path", "latency_us"),
                latency_rows,
            )
            append_rows(
                args.throughput_csv,
                ("kind", "fanout", "hops", "path", "concurrency", "operations", "elapsed_s", "qps"),
                throughput_rows,
            )
            return

        mutation_base = 9_000_000_000 + args.fanout * 1_000_000
        with driver.session(database="default") as session:
            for index in range(args.mutation_iters):
                dst = mutation_base + index
                latency_rows.append(
                    ("write", args.fanout, 0, "hot", timed_mutation(session, WRITE_QUERY, mutation_base, dst))
                )

        concurrent_write_base = mutation_base + 100_000
        write_samples, write_elapsed = concurrent_operations(
            driver,
            args.mutation_concurrency,
            args.mutation_ops_per_worker,
            lambda session, worker, index: timed_mutation(
                session,
                WRITE_QUERY,
                concurrent_write_base,
                concurrent_write_base + worker * args.mutation_ops_per_worker + index,
            ),
        )
        latency_rows.extend(
            ("write_concurrent", args.fanout, 0, "hot", sample)
            for sample in write_samples
        )
        throughput_rows.append(
            (
                "write",
                args.fanout,
                0,
                "hot",
                args.mutation_concurrency,
                len(write_samples),
                write_elapsed,
                len(write_samples) / write_elapsed,
            )
        )

        with driver.session(database="default") as session:
            for index in range(args.mutation_iters):
                dst = mutation_base + index
                latency_rows.append(
                    ("delete", args.fanout, 0, "hot", timed_mutation(session, DELETE_QUERY, mutation_base, dst))
                )

        delete_samples, delete_elapsed = concurrent_operations(
            driver,
            args.mutation_concurrency,
            args.mutation_ops_per_worker,
            lambda session, worker, index: timed_mutation(
                session,
                DELETE_QUERY,
                concurrent_write_base,
                concurrent_write_base + worker * args.mutation_ops_per_worker + index,
            ),
        )
        latency_rows.extend(
            ("delete_concurrent", args.fanout, 0, "hot", sample)
            for sample in delete_samples
        )
        throughput_rows.append(
            (
                "delete",
                args.fanout,
                0,
                "hot",
                args.mutation_concurrency,
                len(delete_samples),
                delete_elapsed,
                len(delete_samples) / delete_elapsed,
            )
        )

    append_rows(
        args.latency_csv,
        ("kind", "fanout", "hops", "path", "latency_us"),
        latency_rows,
    )
    append_rows(
        args.throughput_csv,
        ("kind", "fanout", "hops", "path", "concurrency", "operations", "elapsed_s", "qps"),
        throughput_rows,
    )


def nearest_rank(values, percentile):
    values = sorted(values)
    index = max(0, math.ceil(percentile / 100.0 * len(values)) - 1)
    return values[index]


def summarize(args):
    groups = {}
    with open(args.latency_csv, newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            key = (row["kind"], int(row["fanout"]), int(row["hops"]), row["path"])
            groups.setdefault(key, []).append(float(row["latency_us"]))
    qps = {}
    throughput_path = Path(args.throughput_csv)
    if throughput_path.exists():
        with throughput_path.open(newline="", encoding="utf-8") as stream:
            for row in csv.DictReader(stream):
                key = (row["kind"], int(row["fanout"]), int(row["hops"]), row["path"])
                qps[key] = float(row["qps"])

    output = []
    for key, samples in sorted(groups.items(), key=lambda item: (item[0][1], item[0][2], item[0][0])):
        kind, fanout, hops, path = key
        if kind.endswith("_concurrent"):
            continue
        effective_qps = qps.get(key)
        if effective_qps is None and path == "cold":
            effective_qps = 1_000_000.0 / statistics.mean(samples)
        output.append(
            (
                kind,
                fanout,
                hops,
                path,
                len(samples),
                nearest_rank(samples, 50),
                nearest_rank(samples, 95),
                nearest_rank(samples, 99),
                statistics.mean(samples),
                effective_qps or 0.0,
            )
        )
    target = args.summary_csv or os.devnull
    with open(target, "w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(("kind", "fanout", "hops", "path", "samples", "p50_us", "p95_us", "p99_us", "mean_us", "qps"))
        writer.writerows(output)
    for row in output:
        print(",".join(str(value) for value in row))


def main():
    args = parse_args()
    if args.mode == "cold":
        run_cold(args)
    elif args.mode == "hot":
        run_hot(args)
    else:
        summarize(args)


if __name__ == "__main__":
    main()
