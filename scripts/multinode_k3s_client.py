#!/usr/bin/env python3
import argparse
import json
import re
import socket
import threading
import time
import urllib.request

from neo4j import GraphDatabase, Query, READ_ACCESS, WRITE_ACCESS


TOKEN = "multinode-test-token-with-at-least-32-characters"
DATABASE = "default"
ROUTING_URI = "neo4j://turbolay-bolt.turbolay-multinode.svc.cluster.local:7687"
POD_URIS = [
    f"bolt://turbolay-node-{index}.turbolay-node-headless."
    "turbolay-multinode.svc.cluster.local:7687"
    for index in range(3)
]


def driver(uri):
    return GraphDatabase.driver(
        uri,
        auth=("neo4j", TOKEN),
        connection_timeout=10,
        max_transaction_retry_time=10,
    )


def bookmark_values(bookmarks):
    return sorted(bookmarks.raw_values)


def consume_write(session, query, **parameters):
    result = session.run(query, **parameters)
    summary = result.consume()
    return summary.server.address


def read_one(session, query, **parameters):
    result = session.run(query, **parameters)
    row = result.single(strict=True)
    summary = result.consume()
    return row, summary.server.address


def read_one_strong(session, query, **parameters):
    result = session.run(
        Query(query, metadata={"turbolay.consistency": "strong"}),
        **parameters,
    )
    row = result.single(strict=True)
    summary = result.consume()
    return row, summary.server.address


def seed():
    nodes = [{"vertex": value, "name": f"node-{value}"} for value in range(1, 37)]
    edges = [
        {"src": 1, "dst": value, "relationship": value}
        for value in range(2, 32)
    ]
    edges.extend(
        {"src": source, "dst": destination, "relationship": 100 + offset}
        for offset, (source, destination) in enumerate(
            [(2, 32), (32, 33), (33, 34), (34, 35), (35, 36)]
        )
    )
    with driver(ROUTING_URI) as routed:
        with routed.session(database=DATABASE, default_access_mode=WRITE_ACCESS) as session:
            writer = consume_write(
                session,
                """
                UNWIND $rows AS row
                MERGE (n {id: row.vertex})
                SET n:MultiNode, n.name = row.name
                """,
                rows=nodes,
            )
            consume_write(
                session,
                """
                UNWIND $rows AS row
                MATCH (s:MultiNode {id: row.src}), (d:MultiNode {id: row.dst})
                MERGE (s)-[:MULTI_LINK {id: row.relationship}]->(d)
                """,
                rows=edges,
            )
            bookmarks = session.last_bookmarks()
    print(
        json.dumps(
            {
                "phase": "seed",
                "writer": str(writer),
                "bookmarks": bookmark_values(bookmarks),
            }
        )
    )


def verify_all(expect_extra=False):
    expected_name = "post-restart" if expect_extra else "node-1"
    with driver(ROUTING_URI) as routed:
        with routed.session(database=DATABASE, default_access_mode=READ_ACCESS) as session:
            row, address = read_one_strong(
                session,
                "MATCH (n:MultiNode {id: $id}) RETURN n.name AS name",
                id=1 if not expect_extra else 1000,
            )
            assert row["name"] == expected_name, row
            routed_read = str(address)
    direct = []
    for index, uri in enumerate(POD_URIS):
        with driver(uri) as current:
            with current.session(database=DATABASE) as session:
                row, address = read_one_strong(
                    session,
                    "MATCH (n:MultiNode {id: 1}) RETURN n.name AS name",
                )
                assert row["name"] == "node-1", (index, row)
                direct.append(str(address))
    print(json.dumps({"phase": "verify", "routed_read": routed_read, "direct": direct}))


def bookmark_visibility(value):
    with driver(ROUTING_URI) as routed:
        with routed.session(database=DATABASE, default_access_mode=WRITE_ACCESS) as session:
            writer = consume_write(
                session,
                """
                UNWIND $rows AS row
                MERGE (n {id: row.vertex})
                SET n:MultiNode, n.name = row.name
                """,
                rows=[{"vertex": value, "name": f"bookmark-{value}"}],
            )
            bookmarks = session.last_bookmarks()
    observed = []
    for index, uri in enumerate(POD_URIS):
        with driver(uri) as current:
            with current.session(database=DATABASE, bookmarks=bookmarks) as session:
                row, address = read_one(
                    session,
                    "MATCH (n:MultiNode {id: $id}) RETURN n.name AS name",
                    id=value,
                )
                assert row["name"] == f"bookmark-{value}", (index, row)
                observed.append(str(address))
    print(
        json.dumps(
            {
                "phase": "bookmark",
                "writer": str(writer),
                "bookmarks": bookmark_values(bookmarks),
                "observed": observed,
            }
        )
    )


def strong_visibility(value):
    with driver(ROUTING_URI) as routed:
        with routed.session(database=DATABASE, default_access_mode=WRITE_ACCESS) as session:
            writer = consume_write(
                session,
                """
                UNWIND $rows AS row
                MERGE (n {id: row.vertex})
                SET n:MultiNode, n.name = row.name
                """,
                rows=[{"vertex": value, "name": f"strong-{value}"}],
            )
    observed = []
    query = Query(
        "MATCH (n:MultiNode {id: $id}) RETURN n.name AS name",
        metadata={"turbolay.consistency": "strong"},
    )
    for index, uri in enumerate(POD_URIS):
        with driver(uri) as current:
            with current.session(database=DATABASE) as session:
                row, address = read_one(session, query, id=value)
                assert row["name"] == f"strong-{value}", (index, row)
                observed.append(str(address))
    print(
        json.dumps(
            {"phase": "strong", "writer": str(writer), "observed": observed}
        )
    )


def traversal(expect_tail=False):
    expected = {
        1: set(range(2, 32)),
        2: {32, 36} if expect_tail else {32},
        3: {33},
        4: {34},
        5: {35},
    }
    observed = {}
    for index, uri in enumerate(POD_URIS):
        per_hop = {}
        with driver(uri) as current:
            with current.session(database=DATABASE) as session:
                for hops, expected_vertices in expected.items():
                    result = session.run(
                        f"MATCH ({{id: 1}})-[:MULTI_LINK*{hops}..{hops}]->(v) "
                        "RETURN v.id AS id"
                    )
                    vertices = {row["id"] for row in result}
                    result.consume()
                    assert vertices == expected_vertices, (index, hops, vertices)
                    per_hop[str(hops)] = len(vertices)
        observed[str(index)] = per_hop
    print(json.dumps({"phase": "traversal", "observed": observed}))


def topology_tail(write):
    if write:
        with driver(ROUTING_URI) as routed:
            with routed.session(database=DATABASE, default_access_mode=WRITE_ACCESS) as session:
                consume_write(
                    session,
                    """
                    UNWIND $rows AS row
                    MATCH (s:MultiNode {id: row.src}), (d:MultiNode {id: row.dst})
                    MERGE (s)-[:MULTI_LINK {id: row.relationship}]->(d)
                    """,
                    rows=[{"src": 31, "dst": 36, "relationship": 9999}],
                )
    query = Query(
        "MATCH ({id: 1})-[:MULTI_LINK*2..2]->(v) RETURN v.id AS id",
        metadata={"turbolay.consistency": "strong"},
    )
    observed = {}
    for index, uri in enumerate(POD_URIS):
        with driver(uri) as current:
            with current.session(database=DATABASE) as session:
                result = session.run(query)
                vertices = {row["id"] for row in result}
                result.consume()
                assert vertices == {32, 36}, (index, vertices)
                observed[str(index)] = sorted(vertices)
    print(json.dumps({"phase": "topology-tail", "write": write, "observed": observed}))


def graphblas_metrics():
    observed = {}
    for index in range(3):
        host = (
            f"turbolay-node-{index}.turbolay-node-headless."
            "turbolay-multinode.svc.cluster.local"
        )
        body = urllib.request.urlopen(f"http://{host}:9090/metrics", timeout=5).read().decode()
        artifact = int(
            re.search(
                r'graph_query_graphblas_artifact_snapshots\{cell_id="cell-0"\} (\d+)',
                body,
            ).group(1)
        )
        fallback = int(
            re.search(
                r'graph_query_rust_sparse_fallbacks\{cell_id="cell-0"\} (\d+)', body
            ).group(1)
        )
        assert artifact > 0, (index, artifact)
        assert fallback == 0, (index, fallback)
        observed[str(index)] = {"artifact_snapshots": artifact, "rust_fallbacks": fallback}
    print(json.dumps({"phase": "metrics", "observed": observed}))


def routing_distribution():
    read_addresses = set()
    write_addresses = set()
    with driver(ROUTING_URI) as routed:
        for _ in range(12):
            with routed.session(database=DATABASE, default_access_mode=READ_ACCESS) as session:
                _, address = read_one(
                    session,
                    "MATCH (n:MultiNode {id: 1}) RETURN n.id AS id",
                )
                read_addresses.add(str(address))
        with routed.session(database=DATABASE, default_access_mode=WRITE_ACCESS) as session:
            write_addresses.add(
                str(
                    consume_write(
                        session,
                        """
                        UNWIND $rows AS row
                        MERGE (n {id: row.vertex})
                        SET n:MultiNode, n.name = row.name
                        """,
                        rows=[{"vertex": 900, "name": "routing-writer"}],
                    )
                )
            )
    assert len(read_addresses) >= 2, read_addresses
    assert len(write_addresses) == 1, write_addresses
    expected_writer_ip = socket.gethostbyname(
        "turbolay-node-0.turbolay-node-headless.turbolay-multinode.svc.cluster.local"
    )
    actual_writer_ip = next(iter(write_addresses)).rsplit(":", 1)[0]
    assert actual_writer_ip == expected_writer_ip, (write_addresses, expected_writer_ip)
    print(
        json.dumps(
            {
                "phase": "routing",
                "read_addresses": sorted(read_addresses),
                "write_addresses": sorted(write_addresses),
            }
        )
    )


def concurrent_load():
    failures = []
    start = threading.Barrier(7)

    def reader(index):
        try:
            start.wait()
            with driver(POD_URIS[index % 3]) as current:
                for _ in range(20):
                    with current.session(database=DATABASE) as session:
                        row, _ = read_one(
                            session,
                            "MATCH (n:MultiNode {id: 1}) RETURN n.name AS name",
                        )
                        assert row["name"] == "node-1"
        except Exception as error:  # noqa: BLE001 - test must collect every worker failure
            failures.append(f"reader-{index}: {error!r}")

    def writer():
        try:
            start.wait()
            with driver(ROUTING_URI) as routed:
                for value in range(910, 930):
                    with routed.session(database=DATABASE, default_access_mode=WRITE_ACCESS) as session:
                        consume_write(
                            session,
                            """
                            UNWIND $rows AS row
                            MERGE (n {id: row.vertex})
                            SET n:MultiNode, n.name = row.name
                            """,
                            rows=[{"vertex": value, "name": f"load-{value}"}],
                        )
        except Exception as error:  # noqa: BLE001
            failures.append(f"writer: {error!r}")

    threads = [threading.Thread(target=reader, args=(index,)) for index in range(6)]
    threads.append(threading.Thread(target=writer))
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not failures, failures
    print(json.dumps({"phase": "concurrent", "elapsed_ms": round((time.perf_counter() - started) * 1000, 2)}))


def direct_write(pod_index, value, name):
    with driver(POD_URIS[pod_index]) as current:
        with current.session(database=DATABASE, default_access_mode=WRITE_ACCESS) as session:
            address = consume_write(
                session,
                """
                UNWIND $rows AS row
                MERGE (n {id: row.vertex})
                SET n:MultiNode, n.name = row.name
                """,
                rows=[{"vertex": value, "name": name}],
            )
            bookmarks = session.last_bookmarks()
    print(
        json.dumps(
            {
                "phase": "direct-write",
                "pod": pod_index,
                "writer": str(address),
                "bookmarks": bookmark_values(bookmarks),
            }
        )
    )


def routed_write(value, name):
    with driver(ROUTING_URI) as routed:
        with routed.session(database=DATABASE, default_access_mode=WRITE_ACCESS) as session:
            address = consume_write(
                session,
                """
                UNWIND $rows AS row
                MERGE (n {id: row.vertex})
                SET n:MultiNode, n.name = row.name
                """,
                rows=[{"vertex": value, "name": name}],
            )
    print(json.dumps({"phase": "routed-write", "writer": str(address), "value": value}))


def routed_verify(value, name):
    with driver(ROUTING_URI) as routed:
        with routed.session(database=DATABASE, default_access_mode=READ_ACCESS) as session:
            row, address = read_one_strong(
                session,
                "MATCH (n:MultiNode {id: $id}) RETURN n.name AS name",
                id=value,
            )
            assert row["name"] == name, row
    print(
        json.dumps(
            {
                "phase": "routed-verify",
                "reader": str(address),
                "value": value,
            }
        )
    )


def tail_limit_seed():
    nodes = [{"vertex": 10000, "name": "tail-root"}, {"vertex": 10001, "name": "tail-result"}]
    for offset in range(65):
        nodes.extend(
            [
                {"vertex": 10100 + offset * 2, "name": f"tail-src-{offset}"},
                {"vertex": 10101 + offset * 2, "name": f"tail-dst-{offset}"},
            ]
        )
    with driver(ROUTING_URI) as routed:
        with routed.session(database=DATABASE, default_access_mode=WRITE_ACCESS) as session:
            consume_write(
                session,
                """
                UNWIND $rows AS row
                MERGE (n {id: row.vertex})
                SET n:TailLimit, n.name = row.name
                """,
                rows=nodes,
            )
            consume_write(
                session,
                """
                UNWIND $rows AS row
                MATCH (s:TailLimit {id: row.src}), (d:TailLimit {id: row.dst})
                MERGE (s)-[:TAIL_LIMIT {id: row.relationship}]->(d)
                """,
                rows=[{"src": 10000, "dst": 10001, "relationship": 1}],
            )
    print(json.dumps({"phase": "tail-limit-seed"}))


def tail_limit_overflow():
    rows = [
        {
            "src": 10100 + offset * 2,
            "dst": 10101 + offset * 2,
            "relationship": 100 + offset,
        }
        for offset in range(65)
    ]
    with driver(ROUTING_URI) as routed:
        with routed.session(database=DATABASE, default_access_mode=WRITE_ACCESS) as session:
            consume_write(
                session,
                """
                UNWIND $rows AS row
                MATCH (s:TailLimit {id: row.src}), (d:TailLimit {id: row.dst})
                MERGE (s)-[:TAIL_LIMIT {id: row.relationship}]->(d)
                """,
                rows=rows,
            )
    print(json.dumps({"phase": "tail-limit-overflow", "affected_edges": len(rows)}))


def tail_limit_verify(expect_bound):
    query = Query(
        "MATCH ({id: 10000})-[:TAIL_LIMIT*1..1]->(v) RETURN count(*) AS count",
        metadata={"turbolay.consistency": "strong"},
    )
    try:
        with driver(ROUTING_URI) as routed:
            with routed.session(database=DATABASE, default_access_mode=READ_ACCESS) as session:
                row, address = read_one(session, query)
                assert row["count"] == 1, row
    except Exception as error:  # noqa: BLE001 - the test asserts the server boundary
        if not expect_bound or "graph_index_wal_affected_edges" not in str(error):
            raise
        print(json.dumps({"phase": "tail-limit-bounded", "error": str(error)}))
        return
    if expect_bound:
        raise AssertionError("strong read unexpectedly exceeded its WAL-tail safety bound")
    print(json.dumps({"phase": "tail-limit-recovered", "reader": str(address)}))


def ambiguous_write_loop(count):
    source = 20000
    destination = 20001
    with driver(ROUTING_URI) as routed:
        with routed.session(database=DATABASE, default_access_mode=WRITE_ACCESS) as session:
            consume_write(
                session,
                """
                UNWIND $rows AS row
                MERGE (n {id: row.vertex})
                SET n:AmbiguousWrite, n.name = row.name
                """,
                rows=[
                    {"vertex": source, "name": "ambiguous-source"},
                    {"vertex": destination, "name": "ambiguous-destination"},
                ],
            )
    print(json.dumps({"phase": "ambiguous-start", "count": count}), flush=True)

    ambiguous_failures = 0
    for offset in range(count):
        relationship = 30000 + offset
        while True:
            try:
                with driver(ROUTING_URI) as routed:
                    with routed.session(database=DATABASE, default_access_mode=WRITE_ACCESS) as session:
                        consume_write(
                            session,
                            """
                            UNWIND $rows AS row
                            MATCH (s:AmbiguousWrite {id: row.src}),
                                  (d:AmbiguousWrite {id: row.dst})
                            MERGE (s)-[:AMBIGUOUS_WRITE {id: row.relationship}]->(d)
                            """,
                            rows=[
                                {
                                    "src": source,
                                    "dst": destination,
                                    "relationship": relationship,
                                }
                            ],
                        )
                break
            except Exception:  # noqa: BLE001 - any lost response is explicitly retried
                ambiguous_failures += 1
                time.sleep(0.05)
        time.sleep(0.01)

    query = Query(
        """
        MATCH ({id: $src})-[r:AMBIGUOUS_WRITE {id: $relationship}]->({id: $dst})
        RETURN count(*) AS count
        """,
        metadata={"turbolay.consistency": "strong"},
    )
    with driver(ROUTING_URI) as routed:
        with routed.session(database=DATABASE, default_access_mode=READ_ACCESS) as session:
            for offset in range(count):
                row, _ = read_one(
                    session,
                    query,
                    src=source,
                    dst=destination,
                    relationship=30000 + offset,
                )
                assert row["count"] == 1, (offset, row)
    print(
        json.dumps(
            {
                "phase": "ambiguous-complete",
                "count": count,
                "ambiguous_failures": ambiguous_failures,
            }
        ),
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="phase", required=True)
    subparsers.add_parser("seed")
    verify = subparsers.add_parser("verify")
    verify.add_argument("--expect-extra", action="store_true")
    bookmark = subparsers.add_parser("bookmark")
    bookmark.add_argument("value", type=int)
    strong = subparsers.add_parser("strong")
    strong.add_argument("value", type=int)
    subparsers.add_parser("routing")
    traversal_parser = subparsers.add_parser("traversal")
    traversal_parser.add_argument("--expect-tail", action="store_true")
    subparsers.add_parser("topology-tail")
    subparsers.add_parser("topology-verify")
    subparsers.add_parser("metrics")
    subparsers.add_parser("concurrent")
    direct = subparsers.add_parser("direct-write")
    direct.add_argument("pod", type=int)
    direct.add_argument("value", type=int)
    direct.add_argument("name")
    routed = subparsers.add_parser("routed-write")
    routed.add_argument("value", type=int)
    routed.add_argument("name")
    routed_verify_parser = subparsers.add_parser("routed-verify")
    routed_verify_parser.add_argument("value", type=int)
    routed_verify_parser.add_argument("name")
    subparsers.add_parser("tail-limit-seed")
    subparsers.add_parser("tail-limit-overflow")
    subparsers.add_parser("tail-limit-bounded")
    subparsers.add_parser("tail-limit-recovered")
    ambiguous = subparsers.add_parser("ambiguous-write-loop")
    ambiguous.add_argument("count", type=int)
    args = parser.parse_args()

    if args.phase == "seed":
        seed()
    elif args.phase == "verify":
        verify_all(args.expect_extra)
    elif args.phase == "bookmark":
        bookmark_visibility(args.value)
    elif args.phase == "strong":
        strong_visibility(args.value)
    elif args.phase == "routing":
        routing_distribution()
    elif args.phase == "traversal":
        traversal(args.expect_tail)
    elif args.phase == "topology-tail":
        topology_tail(True)
    elif args.phase == "topology-verify":
        topology_tail(False)
    elif args.phase == "metrics":
        graphblas_metrics()
    elif args.phase == "concurrent":
        concurrent_load()
    elif args.phase == "direct-write":
        direct_write(args.pod, args.value, args.name)
    elif args.phase == "routed-write":
        routed_write(args.value, args.name)
    elif args.phase == "routed-verify":
        routed_verify(args.value, args.name)
    elif args.phase == "tail-limit-seed":
        tail_limit_seed()
    elif args.phase == "tail-limit-overflow":
        tail_limit_overflow()
    elif args.phase == "tail-limit-bounded":
        tail_limit_verify(True)
    elif args.phase == "tail-limit-recovered":
        tail_limit_verify(False)
    elif args.phase == "ambiguous-write-loop":
        ambiguous_write_loop(args.count)


if __name__ == "__main__":
    main()
