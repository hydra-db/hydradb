#!/usr/bin/env python3
import csv
import os
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import neo4j
from neo4j import GraphDatabase


EDGE_TYPE = "USER_FOLLOWS_USER"
DEFAULT_FANOUTS = (10, 50, 100, 1_000, 5_000, 10_000)
DEFAULT_HOPS = (1, 3, 5, 10)
CSV_FIELDS = (
    "kind",
    "backend",
    "kernel",
    "fanout",
    "hops",
    "latency_samples",
    "p50_us",
    "p95_us",
    "p99_us",
    "mean_us",
    "concurrency",
    "concurrent_operations",
    "concurrent_p50_us",
    "concurrent_p95_us",
    "concurrent_p99_us",
    "qps",
    "operations_per_s",
    "reachable_vertices_per_s",
    "logical_edges_per_s",
    "expected_count",
)


def env_int(name, default):
    return max(1, int(os.getenv(name, default)))


def env_int_list(name, default):
    raw = os.getenv(name)
    return tuple(int(value.strip()) for value in raw.split(",")) if raw else default


def percentile(sorted_values, percent):
    if not sorted_values:
        return 0.0
    return sorted_values[(len(sorted_values) - 1) * percent // 100]


@dataclass(frozen=True)
class LatencyStats:
    p50_us: float
    p95_us: float
    p99_us: float
    mean_us: float

    @classmethod
    def from_ns(cls, values):
        micros = sorted(value / 1_000.0 for value in values)
        return cls(
            percentile(micros, 50),
            percentile(micros, 95),
            percentile(micros, 99),
            statistics.fmean(micros) if micros else 0.0,
        )


def run_count(session, query, expected):
    record = session.run(query).single(strict=True)
    actual = record[0]
    if actual != expected:
        raise RuntimeError(f"expected count {expected}, got {actual}")


def run_write(session, query, dst):
    session.run(query, src=1, dst=dst).consume()


def concurrent_run(driver, query, expected, concurrency, operations_per_worker, write=False, first_dst=0):
    barrier = threading.Barrier(concurrency + 1)

    def worker(worker_id):
        latencies = []
        with driver.session() as session:
            session.run("RETURN 1").consume()
            barrier.wait()
            started_all = time.perf_counter_ns()
            for offset in range(operations_per_worker):
                started = time.perf_counter_ns()
                if write:
                    run_write(session, query, first_dst + worker_id * operations_per_worker + offset)
                else:
                    run_count(session, query, expected)
                latencies.append(time.perf_counter_ns() - started)
            return latencies, time.perf_counter_ns() - started_all

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker, worker_id) for worker_id in range(concurrency)]
        barrier.wait()
        results = [future.result() for future in futures]
    latencies = [sample for worker_samples, _ in results for sample in worker_samples]
    return latencies, max(elapsed for _, elapsed in results)


def reset_and_build(driver, fanout, max_hop, chunk_size):
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n").consume()
        session.run("CREATE (:BenchNode {id: 1})").consume()
        path = "(root)"
        for depth in range(max_hop):
            node_id = f"2 + branch * {max_hop} + {depth}"
            path += f"-[:{EDGE_TYPE}]->(:BenchNode {{id: {node_id}}})"
        query = (
            "UNWIND range($start, $end) AS branch "
            "MATCH (root:BenchNode {id: 1}) "
            f"CREATE {path}"
        )
        for start in range(0, fanout, chunk_size):
            end = min(start + chunk_size, fanout) - 1
            session.run(query, start=start, end=end).consume()


def write_row(writer, backend, kernel, kind, fanout, hops, sequential, concurrent,
              concurrency, concurrent_elapsed_ns, expected_count):
    latency = LatencyStats.from_ns(sequential)
    concurrent_latency = LatencyStats.from_ns(concurrent)
    operations = len(concurrent)
    qps = operations / max(concurrent_elapsed_ns / 1_000_000_000.0, sys.float_info.min)
    writer.writerow(
        {
            "kind": kind,
            "backend": backend,
            "kernel": kernel,
            "fanout": fanout,
            "hops": hops,
            "latency_samples": len(sequential),
            "p50_us": f"{latency.p50_us:.3f}",
            "p95_us": f"{latency.p95_us:.3f}",
            "p99_us": f"{latency.p99_us:.3f}",
            "mean_us": f"{latency.mean_us:.3f}",
            "concurrency": concurrency,
            "concurrent_operations": operations,
            "concurrent_p50_us": f"{concurrent_latency.p50_us:.3f}",
            "concurrent_p95_us": f"{concurrent_latency.p95_us:.3f}",
            "concurrent_p99_us": f"{concurrent_latency.p99_us:.3f}",
            "qps": f"{qps:.2f}",
            "operations_per_s": f"{qps:.2f}",
            "reachable_vertices_per_s": f"{fanout * qps:.2f}" if kind == "read" else "0.00",
            "logical_edges_per_s": f"{fanout * hops * qps:.2f}" if kind == "read" else "0.00",
            "expected_count": expected_count,
        }
    )
    sys.stdout.flush()


def main():
    uri = os.getenv("GRAPH_BOLT_BENCH_URI", "bolt://127.0.0.1:7687")
    fanouts = env_int_list("GRAPH_BOLT_BENCH_FANOUTS", DEFAULT_FANOUTS)
    hops = env_int_list("GRAPH_BOLT_BENCH_HOPS", DEFAULT_HOPS)
    max_hop = max(hops)
    read_warmup = env_int("GRAPH_BOLT_BENCH_READ_WARMUP", 10)
    read_latency_iters = env_int("GRAPH_BOLT_BENCH_READ_LATENCY_ITERS", 50)
    read_concurrency = env_int("GRAPH_BOLT_BENCH_READ_CONCURRENCY", 8)
    read_queries_per_worker = env_int("GRAPH_BOLT_BENCH_READ_QUERIES_PER_WORKER", 50)
    write_warmup = env_int("GRAPH_BOLT_BENCH_WRITE_WARMUP", 5)
    write_latency_iters = env_int("GRAPH_BOLT_BENCH_WRITE_LATENCY_ITERS", 30)
    write_concurrency = env_int("GRAPH_BOLT_BENCH_WRITE_CONCURRENCY", 4)
    write_queries_per_worker = env_int("GRAPH_BOLT_BENCH_WRITE_QUERIES_PER_WORKER", 25)
    build_chunk_size = env_int("GRAPH_BOLT_BENCH_BULK_CHUNK", 500)
    backend = os.getenv("GRAPH_BOLT_BENCH_BACKEND", "falkordb-memory-v4.18.11")
    kernel = os.getenv("GRAPH_BOLT_BENCH_KERNEL", "graphblas")

    print(
        f"FalkorDB Bolt benchmark: driver=neo4j/{neo4j.__version__} fanouts={fanouts} "
        f"hops={hops} read_latency_iters={read_latency_iters} "
        f"read_concurrency={read_concurrency} write_latency_iters={write_latency_iters} "
        f"write_concurrency={write_concurrency}",
        file=sys.stderr,
    )
    driver = GraphDatabase.driver(
        uri,
        auth=("falkordb", ""),
        encrypted=False,
        max_connection_pool_size=max(read_concurrency, write_concurrency) + 2,
    )
    driver.verify_connectivity()
    writer = csv.DictWriter(sys.stdout, fieldnames=CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    with driver.session() as session:
        session.run("CREATE INDEX FOR (n:BenchNode) ON (n.id)").consume()

    try:
        for fanout in fanouts:
            build_started = time.perf_counter_ns()
            reset_and_build(driver, fanout, max_hop, build_chunk_size)
            build_ms = (time.perf_counter_ns() - build_started) / 1_000_000.0
            print(
                f"fanout={fanout} stage=ready edges={fanout * max_hop} build_ms={build_ms:.3f}",
                file=sys.stderr,
            )
            for hop in hops:
                query = (
                    f"MATCH (u:BenchNode {{id: 1}})-[:{EDGE_TYPE}*{hop}..{hop}]->(v) "
                    "RETURN count(*) AS total"
                )
                sequential = []
                with driver.session() as session:
                    for _ in range(read_warmup):
                        run_count(session, query, fanout)
                    for _ in range(read_latency_iters):
                        started = time.perf_counter_ns()
                        run_count(session, query, fanout)
                        sequential.append(time.perf_counter_ns() - started)
                concurrent, elapsed = concurrent_run(
                    driver,
                    query,
                    fanout,
                    read_concurrency,
                    read_queries_per_worker,
                )
                write_row(
                    writer,
                    backend,
                    kernel,
                    "read",
                    fanout,
                    hop,
                    sequential,
                    concurrent,
                    read_concurrency,
                    elapsed,
                    fanout,
                )

            write_query = f"CREATE (a {{id: $src}})-[:{EDGE_TYPE}]->(b {{id: $dst}})"
            next_dst = 90_000_000 + fanout * 10_000
            sequential = []
            with driver.session() as session:
                for _ in range(write_warmup):
                    run_write(session, write_query, next_dst)
                    next_dst += 1
                for _ in range(write_latency_iters):
                    started = time.perf_counter_ns()
                    run_write(session, write_query, next_dst)
                    sequential.append(time.perf_counter_ns() - started)
                    next_dst += 1
            concurrent, elapsed = concurrent_run(
                driver,
                write_query,
                1,
                write_concurrency,
                write_queries_per_worker,
                write=True,
                first_dst=next_dst,
            )
            write_row(
                writer,
                backend,
                kernel,
                "write",
                fanout,
                0,
                sequential,
                concurrent,
                write_concurrency,
                elapsed,
                1,
            )
    finally:
        driver.close()


if __name__ == "__main__":
    main()
