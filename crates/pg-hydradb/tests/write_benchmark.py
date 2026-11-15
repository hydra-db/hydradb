"""Times writes that keep the adjacency tables current, and the query after them.

Builds one vertex table and one edge table and registers them, timing the
initial fill. Then for each batch size it updates, then inserts, then deletes
that many edges, timing each statement, which runs the maintenance triggers, and
the next one-hop call, read from the adjacency tables, whose cost is its lookups.
"""

import argparse
import json
import random
import time

from integration import Session, literal, require

GRAPH = "write_benchmark"


def timed(session, sql):
    started = time.perf_counter_ns()
    rows = session.run(sql)
    return (time.perf_counter_ns() - started) / 1_000_000, rows


def query(session, options):
    sql = (f"SELECT hydradb.reachable_vertices({literal(GRAPH)}, '{{\"page\": 1}}'::jsonb, "
           f"{literal(json.dumps(options))}::jsonb)")
    elapsed_ms, rows = timed(session, sql)
    require(len(rows) == 1, rows)
    result = json.loads(rows[0])
    return elapsed_ms, result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--vertices", type=int, default=10_000)
    parser.add_argument("--edges", type=int, default=100_000)
    parser.add_argument("--repeats", type=int, default=3)
    arguments = parser.parse_args()
    vertices, edges = arguments.vertices, arguments.edges
    batches = [min(changed, edges) for changed in (1, 100, 10_000)]
    # Each loaded edge is deleted at most once, so every DELETE batch needs its own edges.
    deleted = sum(batches) * arguments.repeats
    if deleted > edges:
        parser.error(f"--edges {edges} is too few: the DELETE batches remove {deleted} edges "
                     f"({arguments.repeats} repeats of {' + '.join(map(str, batches))})")
    options = {"max_depth": 1, "max_snapshot_bytes": 512 * 1024 * 1024}
    with Session() as writer:
        writer.run("SET statement_timeout = '10min'")
        writer.run("CREATE SCHEMA IF NOT EXISTS hydradb_benchmark")
        writer.run("CREATE TABLE hydradb_benchmark.pages (id integer PRIMARY KEY, slug text NOT NULL)")
        writer.run("CREATE TABLE hydradb_benchmark.links (id bigint PRIMARY KEY, src integer NOT NULL REFERENCES hydradb_benchmark.pages, "
                   "dst integer NOT NULL REFERENCES hydradb_benchmark.pages, kind text NOT NULL)")
        writer.run(f"INSERT INTO hydradb_benchmark.pages SELECT n, 'page-' || lpad(n::text, 7, '0') "
                   f"FROM generate_series(1, {vertices}) n")
        writer.run(f"INSERT INTO hydradb_benchmark.links SELECT n, 1 + (n::bigint * 7919) % {vertices}, "
                   f"1 + (n::bigint * 104729 + 17) % {vertices}, 'link' FROM generate_series(1, {edges}) n")
        fill_ms, _ = timed(writer, f"""SELECT hydradb.create_graph({literal(GRAPH)},
            '[{{"label": "page", "table": "hydradb_benchmark.pages", "key": "id", "sort_key": "slug"}}]',
            '[{{"label": "link", "table": "hydradb_benchmark.links", "key": "id", "kind": "kind",
               "source": "src", "source_label": "page", "target": "dst", "target_label": "page"}}]')""")
        writer.run("VACUUM (ANALYZE) hydradb.graph_vertices, hydradb.graph_adjacency")
        sizes = json.loads(writer.run("""SELECT jsonb_object_agg(relname, pg_total_relation_size(oid)) FROM pg_class
            WHERE oid IN ('hydradb.graph_vertices'::regclass, 'hydradb.graph_adjacency'::regclass)""")[0])
        version = writer.run("SELECT version()")[0]
        print(json.dumps({"benchmark": "maintained adjacency tables", "vertices": vertices,
                          "edges": edges, "server": version, "create_graph_ms": round(fill_ms, 1), "table_bytes": sizes,
                          "timing": "client round trip of the write and of a one-hop reachable_vertices call"}), flush=True)
        rng = random.Random(25)
        with Session() as reader:
            reader.run("SET statement_timeout = '10min'")

            def measure(write, changed, sql):
                write_ms, _ = timed(writer, sql)
                adjacency_ms, indexed = query(reader, options)
                print(json.dumps({"write": write, "edges": changed, "write_ms": round(write_ms, 1),
                                  "adjacency_query_ms": round(adjacency_ms, 1),
                                  "lookups": indexed["storage"]["lookups"]}), flush=True)

            for changed in batches:
                for _ in range(arguments.repeats):
                    # Distinct ids, so the batch updates `changed` edges.
                    ids = ",".join(str(id) for id in rng.sample(range(1, edges + 1), changed))
                    measure("update", changed, f"UPDATE hydradb_benchmark.links SET dst = 1 + dst % {vertices} WHERE id IN ({ids})")
            # New edges take ids past the loaded ones and spread their endpoints like the load.
            next_id = edges + 1
            for changed in batches:
                for _ in range(arguments.repeats):
                    measure("insert", changed, f"INSERT INTO hydradb_benchmark.links SELECT n, 1 + (n * 7919) % {vertices}, "
                            f"1 + (n * 104729 + 17) % {vertices}, 'link' FROM generate_series({next_id}::bigint, {next_id + changed - 1}) n")
                    next_id += changed
            # Deletes take loaded edges, each at most once, so every batch removes `changed` edges.
            loaded = list(range(1, edges + 1))
            rng.shuffle(loaded)
            for changed in batches:
                for _ in range(arguments.repeats):
                    batch, loaded = loaded[:changed], loaded[changed:]
                    ids = ",".join(str(id) for id in batch)
                    measure("delete", changed, f"DELETE FROM hydradb_benchmark.links WHERE id IN ({ids})")


if __name__ == "__main__":
    main()
