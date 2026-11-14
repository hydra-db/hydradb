import argparse
import json
import math
import time

from integration import Session, query_sql, require, single_table_graph


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def sample(session, sql):
    started = time.perf_counter_ns()
    rows = session.run(sql)
    require(len(rows) == 1, rows)
    response = json.loads(rows[0])
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    return elapsed_ms, len(rows[0].encode("utf-8")), response


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=7)
    parser.add_argument("--topology", choices=("cycle", "skewed", "both"), default="both")
    arguments = parser.parse_args()
    require(arguments.iterations >= 1, "iterations must be positive")
    topologies = ("cycle", "skewed") if arguments.topology == "both" else (arguments.topology,)
    with Session() as setup:
        setup.run("CREATE SCHEMA hydradb_benchmark")
        for topology in topologies:
            setup.run(f"CREATE TABLE hydradb_benchmark.{topology}_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL)")
            setup.run(f"CREATE TABLE hydradb_benchmark.{topology}_edges (id bigint PRIMARY KEY, "
                      f"source bigint NOT NULL REFERENCES hydradb_benchmark.{topology}_vertices, "
                      f"target bigint NOT NULL REFERENCES hydradb_benchmark.{topology}_vertices, kind text NOT NULL)")
            setup.run(f"INSERT INTO hydradb_benchmark.{topology}_vertices SELECT n, lpad(n::text, 5, '0') FROM generate_series(1, 10000) n")
            if topology == "cycle":
                setup.run("INSERT INTO hydradb_benchmark.cycle_edges SELECT n, (n - 1) / 6 + 1, (((n - 1) / 6 + 1) % 10000) + 1, 'link' FROM generate_series(1, 60000) n")
            else:
                setup.run("INSERT INTO hydradb_benchmark.skewed_edges SELECT n, n, (n % 10000) + 1, 'link' FROM generate_series(1, 10000) n")
                setup.run("INSERT INTO hydradb_benchmark.skewed_edges SELECT n, floor(power(((n * 7919) % 10000)::numeric / 10000, 3) * 10000)::bigint + 1, ((n::bigint * 104729 + 17) % 10000) + 1, 'link' FROM generate_series(10001, 60000) n")
            setup.run(single_table_graph(f"benchmark_{topology}", f"hydradb_benchmark.{topology}_vertices", f"hydradb_benchmark.{topology}_edges"))
        version = setup.run("SELECT version()")[0]
    print(json.dumps({"benchmark": "parallel-edge cycle and deterministic hub-skewed topology", "topologies": topologies, "vertices_per_graph": 10000,
                      "edges": 60000, "server": version,
                      "timing": "client round trip plus full JSON decoding; excludes backend connection; no SLO claim"}))
    for topology in topologies:
        for mode in ("cold_backend", "repeated_backend"):
            for depth in (5, 8, 10):
                for operation in ("reachable_vertices", "reachable_edges", "paths_between"):
                    sql = query_sql(graph=f"benchmark_{topology}", options={"max_depth": depth},
                                    operation=operation, targets=(depth + 1,) if operation == "paths_between" else None)
                    timings = []
                    if mode == "repeated_backend":
                        with Session() as session:
                            sample(session, sql)
                            for _ in range(arguments.iterations):
                                elapsed, response_bytes, response = sample(session, sql)
                                timings.append(elapsed)
                    else:
                        for _ in range(arguments.iterations):
                            with Session() as session:
                                elapsed, response_bytes, response = sample(session, sql)
                                timings.append(elapsed)
                    if topology == "skewed" and operation == "reachable_vertices":
                        require(len(response["items"]) > 100, "skewed fixture did not expand broadly")
                    print(json.dumps({"topology": topology, "mode": mode, "depth": depth,
                                      "operation": operation, "samples": len(timings),
                                      "p50_ms": round(percentile(timings, 0.50), 3),
                                      "p95_ms": round(percentile(timings, 0.95), 3),
                                      "response_bytes": response_bytes, "response": response}), flush=True)


if __name__ == "__main__":
    main()
