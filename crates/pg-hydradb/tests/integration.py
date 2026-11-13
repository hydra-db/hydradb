import argparse
import hashlib
import json
import os
import queue
import random
import subprocess
import threading
import time
import uuid
from pathlib import Path


def require(condition, message):
    if not condition:
        raise AssertionError(message)


class Session:
    def __init__(self, database=None):
        self.process = subprocess.Popen(
            [os.environ["PGHYDRADB_TEST_PSQL"], "-X", "-qAt", "-P", "pager=off",
             "-v", "ON_ERROR_STOP=0", "-v", "VERBOSITY=verbose"] + (["-d", database] if database else []),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        self.lines = queue.Queue()
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        self.run("SET statement_timeout = '30s'; SET lock_timeout = '5s'")

    def _read(self):
        for line in self.process.stdout:
            self.lines.put(line.rstrip("\n"))
        self.lines.put(None)

    def run(self, sql, error=None):
        marker = "hydradb_test_" + uuid.uuid4().hex
        self.process.stdin.write(sql.rstrip().rstrip(";") + ";\n\\echo " + marker + "\n")
        self.process.stdin.flush()
        lines = []
        while True:
            try:
                line = self.lines.get(timeout=45)
            except queue.Empty as exc:
                self.process.kill()
                raise AssertionError(f"psql timed out: {sql}") from exc
            require(line is not None, f"psql exited during {sql}: {lines}")
            if line == marker:
                break
            lines.append(line)
        failures = [line for line in lines if line.startswith(("ERROR:", "FATAL:", "PANIC:"))]
        if error is not None:
            require(failures and error.lower() in "\n".join(failures).lower(),
                    f"expected error containing {error!r} for {sql}: {lines}")
        else:
            require(not failures, f"SQL failed: {sql}: {lines}")
        return [line for line in lines if line]

    def json(self, sql):
        # A query's notices and warnings print around its one result row.
        messages = ("NOTICE:", "WARNING:", "DETAIL:", "HINT:", "CONTEXT:", "LOCATION:")
        rows = [row for row in self.run(sql) if not row.startswith(messages)]
        require(len(rows) == 1, f"expected one JSON row: {rows}")
        return json.loads(rows[0])

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.write("\\q\n")
            self.process.stdin.flush()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        self.reader.join(timeout=2)
        self.process.stdin.close()
        self.process.stdout.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def single_table_graph(name, vertices, edges):
    """create_graph over one vertex table (id, sort_key) and one edge table (id, source, target, kind)."""
    vertex_tables = [{"label": "vertex", "table": vertices, "key": "id", "sort_key": "sort_key"}]
    edge_tables = [{"label": "edge", "table": edges, "key": "id", "source": "source", "source_label": "vertex",
                    "target": "target", "target_label": "vertex", "kind": "kind"}]
    return f"SELECT hydradb.create_graph({literal(name)}, {literal(json.dumps(vertex_tables))}, {literal(json.dumps(edge_tables))})"


def vertex_graph(name, vertices):
    """create_graph over one vertex table (id, sort_key) and no edges."""
    vertex_tables = [{"label": "vertex", "table": vertices, "key": "id", "sort_key": "sort_key"}]
    return f"SELECT hydradb.create_graph({literal(name)}, {literal(json.dumps(vertex_tables))})"


def seeds_json(keys, label):
    return literal(json.dumps({label: [int(key) for key in keys]})) + "::jsonb"


def query_sql(graph="chain", seeds=(1,), options=None, operation="reachable_vertices", targets=None, label="vertex"):
    arguments = [literal(graph), seeds_json(seeds, label)]
    if targets is not None:
        arguments.append(seeds_json(targets, label))
    arguments.append(literal(json.dumps(options if options is not None else {})) + "::jsonb")
    return f"SELECT hydradb.{operation}({', '.join(arguments)})"


def normalized(result, operation="reachable_vertices"):
    """Walk items in a fixed order: by depth, then label and key. Within a depth
    the adjacency tables order by vertex id. Paths keep their order."""
    key = lambda named: (named["label"], (0, named["key"], "") if isinstance(named["key"], int) else (1, 0, str(named["key"])))
    if operation in ("reachable_vertices", "reachable_edges"):
        items = sorted(result["items"], key=lambda i: (i["depth"], key(i)))
    else:
        items = result["items"]
    return {**result, "items": items}


def require_storage(result):
    """Every query reads the adjacency tables and says how much it read."""
    storage = result["storage"]
    expected = {"vertices", "edges", "estimated_bytes", "lookups"}
    require(set(storage) == expected, result)
    require(all(isinstance(storage[key], int) and storage[key] >= 0 for key in expected), result)


def query(session, **kwargs):
    result = session.json(query_sql(**kwargs))
    require(set(result) == {"items", "truncated", "stats", "storage"}, f"unexpected envelope: {result}")
    require(isinstance(result["items"], list) and isinstance(result["truncated"], list), result)
    require(set(result["stats"]) == {"visited_vertices", "edge_visits", "returned_edges", "result_bytes"}, result)
    require(all(isinstance(value, int) and value >= 0 for value in result["stats"].values()), result)
    require_storage(result)
    return normalized(result, kwargs.get("operation", "reachable_vertices"))


def ids(result):
    return [item["key"] for item in result["items"]]


def chain_path(vertices, edges):
    """A shortest path of the single-table test graphs, as the functions return it."""
    return {"vertices": [named("vertex", v) for v in vertices], "edges": [named("edge", e) for e in edges]}


def test_queries(session):
    first = query(session)
    require(first["storage"]["lookups"] > 0 and first["storage"]["estimated_bytes"] > 0, first)
    whole = query(session, options={"max_depth": 20})
    require(whole["storage"]["vertices"] == len(whole["items"]) == 13, whole)
    deep = query(session, options={"max_depth": 10, "edge_kind": "link"})
    require(deep["items"] == [named("vertex", i, i - 1) for i in range(1, 12)], deep)
    require("depth" in deep["truncated"], deep)
    outgoing = query(session, options={"max_depth": 1})
    require(ids(outgoing) == [1, 2, 50], outgoing)
    require(outgoing["storage"]["vertices"] < whole["storage"]["vertices"], f"a one-hop walk loaded the whole graph: {outgoing}")
    incoming = query(session, seeds=(2,), options={"direction": "incoming", "max_depth": 1})
    require(ids(incoming) == [2, 1], incoming)
    both = query(session, seeds=(2,), options={"direction": "both", "max_depth": 1})
    require(ids(both) == [2, 1, 3], both)
    typed = query(session, options={"edge_kind": "other", "max_depth": 10})
    require(ids(typed) == [1, 50] and not typed["truncated"], typed)
    multi = query(session, seeds=(10, 1, 1), options={"edge_kind": "link", "max_depth": 1})
    require(multi["items"] == [named("vertex", 1, 0), named("vertex", 10, 0),
                               named("vertex", 2, 1), named("vertex", 11, 1)], multi)
    edges = query(session, seeds=(2,), operation="reachable_edges", options={"direction": "both", "max_depth": 1})
    require(edges["items"] == [named("edge", edge, 1) for edge in (1, 2, 100, 101)], edges)
    require(len(set(ids(edges))) == len(edges["items"]), "self-loop emitted twice")
    paths = by_key(session, "paths_between", {"vertex": [1, 2]}, {"vertex": [3, 4]}, options={"max_depth": 10, "edge_kinds": ["link"]}, graph="chain")
    require(paths["items"] == [
        chain_path([1, 2, 3], [1, 2]),
        chain_path([1, 2, 3, 4], [1, 2, 3]),
        chain_path([2, 3], [2]),
        chain_path([2, 3, 4], [2, 3]),
    ], paths)
    reverse = by_key(session, "paths_between", {"vertex": [3]}, {"vertex": [1]}, options={"direction": "incoming"}, graph="chain")
    require(reverse["items"] == [chain_path([3, 2, 1], [2, 1])], reverse)
    unreachable = by_key(session, "paths_between", {"vertex": [1]}, {"vertex": [60]}, options={"max_depth": 20}, graph="chain")
    require(unreachable["items"] == [], unreachable)
    self_path = by_key(session, "paths_between", {"vertex": [2]}, {"vertex": [2]}, graph="chain")
    require(self_path["items"] == [chain_path([2], [])], self_path)
    session.run("BEGIN READ ONLY")
    require(query(session)["items"] == first["items"], "read-only transaction changed result")
    session.run("COMMIT")
    signatures = session.json("SELECT jsonb_agg(jsonb_build_object('stable', provolatile = 's', 'invoker', NOT prosecdef)) FROM pg_proc WHERE oid IN ('hydradb.reachable_vertices(text,jsonb,jsonb)'::regprocedure, 'hydradb.reachable_edges(text,jsonb,jsonb)'::regprocedure, 'hydradb.paths_from(text,jsonb,jsonb)'::regprocedure, 'hydradb.paths_between(text,jsonb,jsonb,jsonb)'::regprocedure)")
    require(len(signatures) == 4 and all(row == {"stable": True, "invoker": True} for row in signatures), signatures)
    # Every query reads the adjacency tables; there is no storage to choose.
    for value in ("scan", "adjacency"):
        session.run(query_sql(options={"storage": value}), error="unknown field `storage`")
    print("query semantics, edge identity, depth 10, partial loading, SQL attributes, and read-only transaction passed")


def test_limits(session):
    for option, value, reason, operation, stat in [
        ("max_vertices", 2, "vertices", "reachable_vertices", "visited_vertices"),
        ("max_edges", 1, "edges", "reachable_edges", "returned_edges"),
        ("max_edge_visits", 1, "edge_visits", "reachable_vertices", "edge_visits"),
    ]:
        result = query(session, options={option: value, "max_depth": 10}, operation=operation)
        require(reason in result["truncated"] and result["stats"][stat] <= value, result)
        # strict returns a result cut by the caller's own limits unchanged and
        # fails on a work or size ceiling the caller did not ask to stop at.
        strict_options = {option: value, "max_depth": 10, "strict": True}
        if reason == "edge_visits":
            session.run(query_sql(operation=operation, options=strict_options),
                        error="strict: the query stopped before the caller's limits: edge_visits")
        else:
            strict = query(session, options=strict_options, operation=operation)
            require(strict["items"] == result["items"] and strict["truncated"] == result["truncated"], (strict, result))
    zero_depth = query(session, options={"max_depth": 0})
    require(ids(zero_depth) == [1] and "depth" in zero_depth["truncated"], zero_depth)
    strict_zero = query(session, options={"max_depth": 0, "strict": True})
    require(strict_zero["items"] == zero_depth["items"] and "depth" in strict_zero["truncated"], strict_zero)
    for value in (False, None):
        relaxed = query(session, options={"max_edge_visits": 1, "max_depth": 10, **({"strict": value} if value is not None else {})})
        require("edge_visits" in relaxed["truncated"], relaxed)
    for value in ("yes", 1, None):
        session.run(query_sql(options={"strict": value}), error="strict must be true or false")
    session.run(query_sql(options={"max_result_bytes": 1}), error="max_result_bytes")
    for options in ({"unknown": 1}, {"direction": "sideways"}, {"max_depth": -1},
                    {"max_depth": 1025}, {"max_vertices": 0}, {"max_vertices": 20001},
                    {"max_edges": 5001}, {"max_edge_visits": 10000001},
                    {"max_result_bytes": 67108865}, {"max_sources": 0}, {"max_targets": 1},
                    {"max_snapshot_vertices": 1000}, {"max_snapshot_edges": 1000},
                    {"max_snapshot_bytes": "lots"}, [], None):
        sql = query_sql(options=options)
        if options is None:
            sql = query_sql().replace("'{}'::jsonb", "'null'::jsonb")
        session.run(sql, error="ERROR")
    session.run(query_sql(options={"max_snapshot_bytes": 1}), error="snapshot")
    # The byte budget bounds what a query loads: a one-hop walk fits exactly
    # the bytes it reports, and fails a byte short.
    size = query(session, options={"max_depth": 1})["storage"]["estimated_bytes"]
    exact = query(session, options={"max_depth": 1, "max_snapshot_bytes": size})
    require(ids(exact) == [1, 2, 50], exact)
    session.run(query_sql(options={"max_depth": 1, "max_snapshot_bytes": size - 1}), error="snapshot byte budget exceeded")
    session.run(query_sql(seeds=(1, 2), options={"max_sources": 1}), error="limit")
    session.run(query_sql(seeds=()), error="empty")
    session.run(query_sql(seeds=(999999,)), error="missing vertex vertex 999999")
    session.run(query_sql(graph="absent"), error="ERROR")
    print("query caps, snapshot budgets over what a query loads, and invalid inputs passed")


def adjacency_matches_sources(session, graph):
    """The adjacency tables hold exactly the graph's edge rows, with their kinds,
    and the vertex keys with their row counts and sort keys."""
    mismatches = session.run(f"""
        WITH g AS (SELECT registration AS id FROM hydradb.graphs WHERE name = {literal(graph)}),
        expected AS (
            SELECT e.ordinal AS label, k.key, s.vid AS source, t.vid AS target, k.kind
            FROM hydradb.graph_edge_tables e
            CROSS JOIN LATERAL jsonb_to_recordset(hydradb_test.edge_rows(e.relation, e.key_column, e.source_column, e.target_column, e.kind_column))
                AS k(key text, src text, dst text, kind text)
            JOIN hydradb.graph_vertex_tables sv ON sv.graph = e.graph AND sv.label = e.source_label
            JOIN hydradb.graph_vertex_tables tv ON tv.graph = e.graph AND tv.label = e.target_label
            JOIN hydradb.graph_vertices s ON s.graph = (SELECT id FROM g) AND s.label = sv.ordinal AND s.key = k.src
            JOIN hydradb.graph_vertices t ON t.graph = (SELECT id FROM g) AND t.label = tv.ordinal AND t.key = k.dst
            WHERE e.graph = {literal(graph)}),
        actual AS (SELECT label, key, source, target, kind FROM hydradb.graph_adjacency WHERE graph = (SELECT id FROM g)),
        edge_rows AS (SELECT count(*) AS n FROM hydradb.graph_edge_tables e
            CROSS JOIN LATERAL jsonb_array_elements(hydradb_test.edge_rows(e.relation, e.key_column, e.source_column, e.target_column, e.kind_column)) r
            WHERE e.graph = {literal(graph)})
        SELECT (SELECT count(*) FROM ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
                                      UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) d)
             + abs((SELECT n FROM edge_rows) - (SELECT count(*) FROM actual))""")
    require(mismatches == ["0"], f"graph {graph}: adjacency rows differ from the edge tables: {mismatches}")
    counts = session.run(f"""
        WITH g AS (SELECT registration AS id FROM hydradb.graphs WHERE name = {literal(graph)}),
        expected AS (
            SELECT v.ordinal AS label, k.key, k.n, k.sort_key FROM hydradb.graph_vertex_tables v
            CROSS JOIN LATERAL jsonb_to_recordset(hydradb_test.key_counts(v.relation, v.key_column, v.sort_key_column))
                AS k(key text, n bigint, sort_key text)
            WHERE v.graph = {literal(graph)}),
        actual AS (SELECT label::bigint AS label, key, rows::bigint AS n, sort_key FROM hydradb.graph_vertices
            WHERE graph = (SELECT id FROM g) AND rows > 0)
        SELECT count(*) FROM ((SELECT label::bigint, key, n, sort_key FROM expected EXCEPT ALL SELECT * FROM actual)
                              UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT label::bigint, key, n, sort_key FROM expected)) d""")
    require(counts == ["0"], f"graph {graph}: vertex row counts or sort keys differ from the vertex tables: {counts}")


def install_checks(admin):
    """Helpers that read a declared table's graph columns as text."""
    admin.run("""CREATE FUNCTION hydradb_test.edge_rows(relation regclass, k name, s name, t name, kind name) RETURNS jsonb LANGUAGE plpgsql AS $$
        DECLARE result jsonb; BEGIN
        EXECUTE format('SELECT coalesce(jsonb_agg(jsonb_build_object(''key'', %I::text, ''src'', %I::text, ''dst'', %I::text, ''kind'', %s)), ''[]'') FROM %s',
            k, s, t, CASE WHEN kind IS NULL THEN 'NULL::text' ELSE format('%I::text', kind) END, relation) INTO result;
        RETURN result; END $$""")
    admin.run("""CREATE FUNCTION hydradb_test.key_counts(relation regclass, k name, sort name) RETURNS jsonb LANGUAGE plpgsql AS $$
        DECLARE result jsonb; BEGIN
        EXECUTE format('SELECT coalesce(jsonb_agg(jsonb_build_object(''key'', key, ''n'', n, ''sort_key'', sort_key)), ''[]'') FROM (SELECT %I::text AS key, count(*) AS n, min(%s) AS sort_key FROM %s GROUP BY 1) c',
            k, CASE WHEN sort IS NULL THEN 'NULL::text' ELSE format('%I::text', sort) END, relation) INTO result;
        RETURN result; END $$""")


def test_mutations(reader, writer):
    original = query(reader, graph="mutable")
    require(ids(original) == [1, 2], original)
    writer.run("BEGIN; INSERT INTO hydradb_test.mutable_edges VALUES (2, 2, 3, 'link')")
    require(ids(query(reader, graph="mutable")) == [1, 2], "an uncommitted edge was visible to another session")
    writer.run("ROLLBACK")
    require(ids(query(reader, graph="mutable")) == [1, 2], "a rolled-back edge stayed")
    writer.run("INSERT INTO hydradb_test.mutable_edges VALUES (2, 2, 3, 'link')")
    require(ids(query(reader, graph="mutable")) == [1, 2, 3], "a committed insert was not visible")
    writer.run("UPDATE hydradb_test.mutable_edges SET target = 4 WHERE id = 2")
    require(ids(query(reader, graph="mutable")) == [1, 2, 4], "a committed update was not visible")
    writer.run("UPDATE hydradb_test.mutable_vertices SET sort_key = 'new' WHERE id = 4")
    query(reader, graph="mutable")
    writer.run("DELETE FROM hydradb_test.mutable_edges WHERE id = 2")
    require(ids(query(reader, graph="mutable")) == [1, 2], "a committed delete was not visible")
    writer.run("BEGIN; INSERT INTO hydradb_test.mutable_vertices VALUES (5, 'e'); INSERT INTO hydradb_test.mutable_edges VALUES (5, 2, 5, 'link'); COMMIT")
    require(ids(query(reader, graph="mutable")) == [1, 2, 5], "committed vertex and edge insert not visible")
    writer.run("BEGIN; DELETE FROM hydradb_test.mutable_edges WHERE id = 5; DELETE FROM hydradb_test.mutable_vertices WHERE id = 5; COMMIT")
    require(ids(query(reader, graph="mutable")) == [1, 2], "committed vertex delete not visible")
    adjacency_matches_sources(writer, "mutable")
    writer.run("TRUNCATE hydradb_test.mutable_edges")
    truncated = query(reader, graph="mutable")
    require(ids(truncated) == [1] and truncated["storage"]["edges"] == 0, truncated)
    adjacency_matches_sources(writer, "mutable")
    # An assigned XID without writes no longer matters.
    writer.run("BEGIN; SELECT pg_current_xact_id()")
    require(ids(query(writer, graph="mutable")) == [1], "a transaction with an XID could not query")
    writer.run("ROLLBACK")
    writer.run("ALTER TABLE hydradb_test.mutable_vertices ADD COLUMN metadata text")
    require(ids(query(reader, graph="mutable")) == [1], "adding an undeclared column broke the graph")
    writer.run("ALTER TABLE hydradb_test.mutable_edges RENAME COLUMN target TO old_target",
               error="cannot rename column target of hydradb_test.mutable_edges")
    require(ids(query(reader, graph="mutable")) == [1], "a refused rename broke the graph")
    print("rollback, committed mutations, truncate and schema changes are visible at once")


def test_own_writes(admin):
    admin.run("CREATE TABLE hydradb_test.savepoint_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.savepoint_edges (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.savepoint_vertices, target bigint NOT NULL REFERENCES hydradb_test.savepoint_vertices, kind text NOT NULL)")
    admin.run("INSERT INTO hydradb_test.savepoint_vertices VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
    admin.run("INSERT INTO hydradb_test.savepoint_edges VALUES (1, 1, 2, 'link')")
    admin.run(single_table_graph("savepoints", "hydradb_test.savepoint_vertices", "hydradb_test.savepoint_edges"))
    with Session() as writer, Session() as other:
        for isolation in ("READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"):
            writer.run(f"BEGIN ISOLATION LEVEL {isolation}; INSERT INTO hydradb_test.savepoint_edges VALUES (2, 1, 3, 'link')")
            own = query(writer, graph="savepoints")
            require(ids(own) == [1, 2, 3], f"{isolation}: own insert not visible: {own}")
            require(ids(query(other, graph="savepoints")) == [1, 2], f"{isolation}: another session saw an uncommitted edge")
            writer.run("SAVEPOINT s; INSERT INTO hydradb_test.savepoint_vertices VALUES (5, 'e'); INSERT INTO hydradb_test.savepoint_edges VALUES (3, 3, 5, 'link')")
            require(ids(query(writer, graph="savepoints", options={"max_depth": 10})) == [1, 2, 3, 5], f"{isolation}: own writes after a savepoint not visible")
            writer.run("ROLLBACK TO s")
            require(ids(query(writer, graph="savepoints", options={"max_depth": 10})) == [1, 2, 3], f"{isolation}: rolled-back savepoint still visible")
            writer.run("DELETE FROM hydradb_test.savepoint_edges WHERE id = 1")
            require(ids(query(writer, graph="savepoints")) == [1, 3], f"{isolation}: own delete not visible")
            # One statement: the graph sees what the statement's SQL sees.
            joined = writer.run("""SELECT count(*) FROM jsonb_to_recordset(hydradb.reachable_vertices('savepoints', '{"vertex": [1]}')->'items') AS hit(key bigint, depth integer)
                JOIN hydradb_test.savepoint_vertices v ON v.id = hit.key""")
            require(joined == ["2"], f"{isolation}: joined statement disagreed with the graph: {joined}")
            writer.run("ROLLBACK")
            require(ids(query(writer, graph="savepoints")) == [1, 2], f"{isolation}: rolled-back writes stayed")
        adjacency_matches_sources(admin, "savepoints")
    print("a transaction's graph queries see its own inserts, deletes and savepoint rollbacks at every isolation level; others do not")


def test_replica_role_writes(admin):
    admin.run("CREATE TABLE hydradb_test.replica_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.replica_edges (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.replica_vertices, target bigint NOT NULL REFERENCES hydradb_test.replica_vertices, kind text NOT NULL)")
    admin.run("INSERT INTO hydradb_test.replica_vertices VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    admin.run("INSERT INTO hydradb_test.replica_edges VALUES (1, 1, 2, 'link')")
    admin.run(single_table_graph("replica", "hydradb_test.replica_vertices", "hydradb_test.replica_edges"))
    require(admin.run("SELECT count(*) = 8 AND bool_and(tgenabled = 'A') FROM pg_trigger WHERE tgrelid IN ('hydradb_test.replica_vertices'::regclass, 'hydradb_test.replica_edges'::regclass) AND tgname LIKE 'pg_hydradb_%'") == ["t"], "maintenance triggers are not ENABLE ALWAYS")
    with Session() as reader:
        admin.run("BEGIN; SET LOCAL session_replication_role = replica; INSERT INTO hydradb_test.replica_edges VALUES (2, 2, 3, 'link'); ROLLBACK")
        require(ids(query(reader, graph="replica")) == [1, 2], "a rolled-back replica-role write stayed")
        admin.run("BEGIN; SET LOCAL session_replication_role = replica; INSERT INTO hydradb_test.replica_edges VALUES (2, 2, 3, 'link'); COMMIT")
        require(ids(query(reader, graph="replica")) == [1, 2, 3], "a committed replica-role write was not maintained")
        # A disabled trigger leaves the adjacency tables behind, so queries
        # fail, naming the table, until refresh_graph rebuilds them.
        admin.run("ALTER TABLE hydradb_test.replica_edges DISABLE TRIGGER ALL")
        admin.run("INSERT INTO hydradb_test.replica_edges VALUES (3, 3, 1, 'link')")
        reader.run(query_sql(graph="replica", options={"max_depth": 10}),
                   error="a pg_hydradb trigger on hydradb_test.replica_edges stopped firing for every write")
        prefix = "pg_hydradb_" + hashlib.md5(b"replica").hexdigest()
        for suffix in ("_i", "_d", "_r", "_t"):
            admin.run(f"ALTER TABLE hydradb_test.replica_edges ENABLE ALWAYS TRIGGER {prefix}{suffix}")
        admin.run("SELECT hydradb.refresh_graph('replica')")
        adjacency_matches_sources(admin, "replica")
        require(ids(query(reader, graph="replica", options={"max_depth": 10})) == [1, 2, 3], "refresh_graph did not catch up")
    print("replica-role writes are maintained, rollback stays invisible, disabled triggers stop queries, refresh_graph rebuilds")


def test_refresh_restores_triggers(admin):
    """refresh_graph puts back the triggers a graph lost, so a refresh run while
    one is still disabled or dropped leaves no later write unmirrored; and a
    dropped trigger flags the graph, so creating it again by hand does not let
    queries read adjacency tables that missed the writes in between."""
    create_tables(admin, "hydradb_test.restored_vertices", "hydradb_test.restored_edges")
    admin.run("INSERT INTO hydradb_test.restored_vertices VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
    admin.run("INSERT INTO hydradb_test.restored_edges VALUES (1, 1, 2, 'link')")
    admin.run(single_table_graph("restored", "hydradb_test.restored_vertices", "hydradb_test.restored_edges"))
    prefix = "pg_hydradb_" + hashlib.md5(b"restored").hexdigest()
    always = (f"SELECT count(*) FILTER (WHERE tgenabled = 'A') FROM pg_trigger "
              f"WHERE tgrelid = 'hydradb_test.restored_edges'::regclass AND tgname LIKE '{prefix}%'")
    deep = {"max_depth": 10}

    # Refreshed while its triggers are still disabled: the refresh enables
    # them, so the next write is mirrored.
    admin.run("ALTER TABLE hydradb_test.restored_edges DISABLE TRIGGER USER")
    admin.run("SELECT hydradb.refresh_graph('restored')")
    require(admin.run(always) == ["4"], "refresh_graph left the triggers disabled")
    admin.run("INSERT INTO hydradb_test.restored_edges VALUES (2, 2, 3, 'link')")
    adjacency_matches_sources(admin, "restored")
    require(ids(query(admin, graph="restored", options=deep)) == [1, 2, 3], "a write after the refresh was not mirrored")

    # A dropped trigger flags the graph; created again by hand, it does not
    # clear the flag, since the write in between was never mirrored.
    admin.run(f"DROP TRIGGER {prefix}_i ON hydradb_test.restored_edges")
    lost = f"lost pg_hydradb trigger {prefix}_i on hydradb_test.restored_edges"
    admin.run(query_sql(graph="restored", options=deep), error=lost)
    admin.run("INSERT INTO hydradb_test.restored_edges VALUES (3, 3, 4, 'link')")
    admin.run(f"CREATE TRIGGER {prefix}_i AFTER INSERT ON hydradb_test.restored_edges REFERENCING NEW TABLE AS new_rows "
              f"FOR EACH STATEMENT EXECUTE FUNCTION hydradb.maintain_rows('restored')")
    admin.run(f"ALTER TABLE hydradb_test.restored_edges ENABLE ALWAYS TRIGGER {prefix}_i")
    admin.run(query_sql(graph="restored", options=deep), error=lost)
    admin.run("SELECT hydradb.refresh_graph('restored')")
    require(ids(query(admin, graph="restored", options=deep)) == [1, 2, 3, 4], "refresh missed the write made without the trigger")

    # Refreshed while the trigger is still dropped: the refresh creates it.
    admin.run(f"DROP TRIGGER {prefix}_i ON hydradb_test.restored_edges")
    admin.run("SELECT hydradb.refresh_graph('restored')")
    require(admin.run(always) == ["4"], "refresh_graph did not create the dropped trigger")
    admin.run("INSERT INTO hydradb_test.restored_edges VALUES (4, 4, 5, 'link')")
    adjacency_matches_sources(admin, "restored")
    require(ids(query(admin, graph="restored", options=deep)) == [1, 2, 3, 4, 5], "a write after the refresh was not mirrored")

    # drop_graph's own trigger drops leave no trigger behind. It skips them by
    # flagging the graph it removes, so a graph registered again under the
    # name and losing a trigger in the same transaction is still flagged.
    admin.run("BEGIN")
    admin.run("SELECT hydradb.drop_graph('restored')")
    require(admin.run(f"SELECT count(*) FROM pg_trigger WHERE tgname LIKE '{prefix}%'") == ["0"], "drop_graph left triggers")
    admin.run(single_table_graph("restored", "hydradb_test.restored_vertices", "hydradb_test.restored_edges"))
    admin.run(f"DROP TRIGGER {prefix}_i ON hydradb_test.restored_edges")
    require(admin.run("SELECT needs_refresh IS NOT NULL FROM hydradb.graphs WHERE name = 'restored'") == ["t"],
            "a trigger dropped after drop_graph in the same transaction did not flag the graph")
    admin.run("COMMIT")
    admin.run("SELECT hydradb.drop_graph('restored')")
    print("refresh_graph restores disabled and dropped triggers; a dropped trigger flags its graph until refresh_graph")


def create_tables(admin, vertices, edges):
    admin.run(f"CREATE TABLE {vertices} (id bigint PRIMARY KEY, sort_key text NOT NULL)")
    admin.run(f"CREATE TABLE {edges} (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES {vertices}, "
              f"target bigint NOT NULL REFERENCES {vertices}, kind text NOT NULL)")


def test_streamed_snapshot_budget(admin):
    create_tables(admin, "hydradb_test.large_vertices", "hydradb_test.no_edges")
    create_tables(admin, "hydradb_test.two_vertices", "hydradb_test.large_edges")
    admin.run("INSERT INTO hydradb_test.large_vertices SELECT n, repeat('x', 1024) FROM generate_series(1, 500) n")
    admin.run("INSERT INTO hydradb_test.two_vertices VALUES (1, '1'), (2, '2')")
    admin.run("INSERT INTO hydradb_test.large_edges SELECT n, 1, 2, repeat('x', 1024) FROM generate_series(1, 500) n")
    admin.run(single_table_graph("large_vertices", "hydradb_test.large_vertices", "hydradb_test.no_edges"))
    admin.run(single_table_graph("large_edges", "hydradb_test.two_vertices", "hydradb_test.large_edges"))
    with Session() as reader:
        # From the adjacency tables, vertex 1 of large_vertices fits and its
        # 500 parallel edges of large_edges do not.
        require(ids(query(reader, graph="large_vertices", options={"max_snapshot_bytes": 4096})) == [1], "one vertex did not fit")
        reader.run(query_sql(graph="large_edges", options={"max_snapshot_bytes": 4096}), error="snapshot byte budget exceeded")
        require(reader.run("SELECT count(*) FROM pg_cursors") == ["0"], "a budget error leaked its SPI cursor")
    print("vertex and edge byte budgets bound what a walk loads")


def test_snapshot_field_limits(admin):
    admin.run("CREATE TABLE hydradb_test.oversized_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.oversized_edges (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.two_vertices, "
              "target bigint NOT NULL REFERENCES hydradb_test.two_vertices, kind text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.boundary_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("INSERT INTO hydradb_test.oversized_vertices VALUES (1, repeat('v', 65537))")
    admin.run("INSERT INTO hydradb_test.boundary_vertices VALUES (1, repeat('v', 65536))")
    admin.run("INSERT INTO hydradb_test.oversized_edges VALUES (1, 1, 2, repeat('e', 65537))")
    admin.run(vertex_graph("oversized_vertices", "hydradb_test.oversized_vertices"))
    admin.run(vertex_graph("boundary_vertices", "hydradb_test.boundary_vertices"))
    admin.run(single_table_graph("oversized_edges", "hydradb_test.two_vertices", "hydradb_test.oversized_edges"))
    with Session() as reader:
        for graph in ("oversized_vertices", "oversized_edges"):
            reader.run(query_sql(graph=graph), error="65536-byte field limit")
            require(reader.run("SELECT count(*) FROM pg_cursors") == ["0"], "oversized source leaked its SPI cursor")
        boundary = query(reader, graph="boundary_vertices")
        require(ids(boundary) == [1] and boundary["storage"]["estimated_bytes"] == 65536 + 128 + 8, boundary)
        reader.run(query_sql(graph="boundary_vertices", options={"max_snapshot_bytes": 65536}),
                   error="snapshot byte budget exceeded")
    # Fetches are sized to what the byte budget leaves: with a kind column a row
    # may hold 64 KiB, so a 1 MiB budget reads this hop about 15 rows at a time.
    create_tables(admin, "hydradb_test.fat_vertices", "hydradb_test.fat_edges")
    admin.run("INSERT INTO hydradb_test.fat_vertices SELECT n, lpad(n::text, 3, '0') FROM generate_series(0, 50) n")
    admin.run("INSERT INTO hydradb_test.fat_edges SELECT n, 0, n, repeat('k', 1000) FROM generate_series(1, 50) n")
    admin.run(single_table_graph("fat_kinds", "hydradb_test.fat_vertices", "hydradb_test.fat_edges"))
    fat = query(admin, graph="fat_kinds", seeds=(0,), options={"max_depth": 1, "max_snapshot_bytes": 1024 * 1024})
    require(len(fat["items"]) == 51, fat)
    print("snapshot sort-key/type field limits reject oversized values and accept the exact boundary; "
          "fetches are sized to the byte budget")


def test_permissions(admin):
    admin.run("CREATE FUNCTION hydradb_test.has_table_privilege(oid, text) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT true'")
    with Session() as caller:
        caller.run("SET ROLE hydradb_test_reader")
        caller.run("SET search_path = hydradb_test, public, pg_catalog")
        caller.run("CREATE TEMP TABLE pg_class (oid oid)")
        caller.run(single_table_graph("not_admin", "hydradb_test.vertices", "hydradb_test.edges"), error="permission denied")
        query(caller)
        for table in ("graph_vertices", "graph_adjacency"):
            caller.run(f"SELECT count(*) FROM hydradb.{table}", error="permission denied")
        admin.run("REVOKE SELECT ON hydradb_test.edges FROM hydradb_test_reader")
        require(caller.run("SELECT has_table_privilege('hydradb_test.edges'::regclass::oid, 'SELECT')") == ["t"], "shadow privilege helper did not resolve through attacker search_path")
        caller.run(query_sql(), error="privileges")
        admin.run("GRANT SELECT ON hydradb_test.edges TO hydradb_test_reader")
        require(ids(query(caller))[:2] == [1, 2], "restored SELECT did not restore access")
        admin.run("REVOKE USAGE ON SCHEMA hydradb_test FROM hydradb_test_reader")
        caller.run(query_sql(), error="privileges")
        admin.run("GRANT USAGE ON SCHEMA hydradb_test TO hydradb_test_reader")
        # Row-level security applies to the reader, and the adjacency tables
        # hold every row, so every query of the graph is refused, naming the
        # table, rather than answered with rows the policies hide.
        rls = "row-level security on private_vertices applies to it"
        for _ in range(2):
            caller.run(query_sql(graph="private", options={"max_depth": 10}), error=rls)
        by_key(caller, "paths_between", {"vertex": [1]}, {"vertex": [3]}, graph="private", error=rls)
        caller.run(query_sql(graph="private", operation="reachable_edges"), error=rls)
        # The same backend, as a superuser the policies do not apply to, reads
        # every row from the adjacency tables; back as the reader, the cached
        # graph is not used and the query is refused again.
        caller.run("RESET ROLE")
        for _ in range(2):
            bypassing = query(caller, graph="private", options={"max_depth": 10})
            require(ids(bypassing) == [1, 2, 3], bypassing)
        caller.run("SET ROLE hydradb_test_reader")
        caller.run(query_sql(graph="private", options={"max_depth": 10}), error=rls)
    print("admin registration, search_path attacks, private adjacency tables, privilege revocation, refusal for callers "
          "row-level security applies to, and adjacency reads for RLS-bypassing callers in the same backend passed")


def test_registration_owner(admin):
    """create_graph, refresh_graph and drop_graph run as the extension owner,
    so a role granted EXECUTE on them may still use them only over tables its
    login role owns."""
    functions = ("hydradb.create_graph(text, jsonb, jsonb), hydradb.refresh_graph(text), hydradb.drop_graph(text)")
    admin.run("CREATE ROLE hydradb_test_grantee")
    admin.run(f"GRANT EXECUTE ON FUNCTION {functions} TO hydradb_test_grantee")
    admin.run("GRANT USAGE ON SCHEMA hydradb_test TO hydradb_test_grantee")
    create_tables(admin, "hydradb_test.own_vertices", "hydradb_test.own_edges")
    admin.run("INSERT INTO hydradb_test.own_vertices VALUES (1, 'a')")
    admin.run("ALTER TABLE hydradb_test.own_vertices OWNER TO hydradb_test_grantee")
    admin.run("ALTER TABLE hydradb_test.own_edges OWNER TO hydradb_test_grantee")
    with Session() as grantee:
        grantee.run("SET SESSION AUTHORIZATION hydradb_test_grantee")
        refused = "is owned by hydradb_test_admin, and only its owner may register, refresh or drop a graph over it"
        grantee.run(single_table_graph("not_mine", "hydradb_test.vertices", "hydradb_test.edges"), error=refused)
        grantee.run(single_table_graph("half_mine", "hydradb_test.own_vertices", "hydradb_test.edges"), error=refused)
        grantee.run("SELECT hydradb.refresh_graph('chain')", error=refused)
        grantee.run("SELECT hydradb.drop_graph('chain')", error=refused)
        require(admin.run("SELECT count(*) FROM pg_trigger WHERE tgrelid = 'hydradb_test.vertices'::regclass AND tgname LIKE 'pg_hydradb_%'")
                == admin.run("SELECT count(*) * 4 FROM hydradb.graph_vertex_tables WHERE relation = 'hydradb_test.vertices'::regclass"),
                "a refused registration added triggers")
        grantee.run(single_table_graph("mine", "hydradb_test.own_vertices", "hydradb_test.own_edges"))
        grantee.run("SELECT hydradb.refresh_graph('mine')")
        # An owner that changes while refresh_graph waits for its tables is
        # checked again once they are locked: the blocker holds own_edges, the
        # ownership change queues behind it, and refresh_graph retries after.
        with Session() as blocker, Session() as changer:
            blocker.run("BEGIN; INSERT INTO hydradb_test.own_edges VALUES (1, 1, 1, 'link')")
            outcome = {}

            def refresh():
                try:
                    grantee.run("SELECT hydradb.refresh_graph('mine')")
                except AssertionError as error:
                    outcome["error"] = str(error)

            refresher = threading.Thread(target=refresh)
            refresher.start()
            time.sleep(0.3)
            reowner = threading.Thread(target=lambda: changer.run("ALTER TABLE hydradb_test.own_edges OWNER TO hydradb_test_admin"))
            reowner.start()
            time.sleep(0.3)
            blocker.run("ROLLBACK")
            reowner.join()
            refresher.join()
            require("own_edges is owned by hydradb_test_admin" in outcome.get("error", ""), outcome)
        admin.run("ALTER TABLE hydradb_test.own_edges OWNER TO hydradb_test_grantee")
        grantee.run("SELECT hydradb.refresh_graph('mine')")
        grantee.run("SELECT hydradb.drop_graph('mine')")
    require(admin.run("SELECT count(*) FROM hydradb.graphs WHERE name = 'chain'") == ["1"], "a non-owner dropped graph chain")
    # From PostgreSQL 16 a membership can grant neither the owner's privileges
    # nor SET ROLE to it; such a member is no owner.
    if int(admin.run("SHOW server_version_num")[0]) >= 160000:
        admin.run("CREATE ROLE hydradb_test_owners")
        admin.run("ALTER TABLE hydradb_test.own_vertices OWNER TO hydradb_test_owners")
        admin.run("ALTER TABLE hydradb_test.own_edges OWNER TO hydradb_test_owners")
        admin.run("CREATE ROLE hydradb_test_member")
        admin.run(f"GRANT EXECUTE ON FUNCTION {functions} TO hydradb_test_member")
        admin.run("GRANT hydradb_test_owners TO hydradb_test_member WITH INHERIT FALSE, SET FALSE")
        with Session() as member:
            member.run("SET SESSION AUTHORIZATION hydradb_test_member")
            member.run(single_table_graph("membership", "hydradb_test.own_vertices", "hydradb_test.own_edges"),
                       error="is owned by hydradb_test_owners, and only its owner may")
            admin.run("REVOKE hydradb_test_owners FROM hydradb_test_member")
            admin.run("GRANT hydradb_test_owners TO hydradb_test_member WITH INHERIT FALSE, SET TRUE")
            member.run(single_table_graph("membership", "hydradb_test.own_vertices", "hydradb_test.own_edges"))
            member.run("SELECT hydradb.drop_graph('membership')")
        admin.run("ALTER TABLE hydradb_test.own_vertices OWNER TO hydradb_test_grantee")
        admin.run("ALTER TABLE hydradb_test.own_edges OWNER TO hydradb_test_grantee")
        admin.run(f"REVOKE EXECUTE ON FUNCTION {functions} FROM hydradb_test_member")
        admin.run("DROP ROLE hydradb_test_member")
        admin.run("DROP ROLE hydradb_test_owners")
    admin.run("DROP TABLE hydradb_test.own_edges, hydradb_test.own_vertices")
    admin.run(f"REVOKE EXECUTE ON FUNCTION {functions} FROM hydradb_test_grantee")
    admin.run("REVOKE USAGE ON SCHEMA hydradb_test FROM hydradb_test_grantee")
    admin.run("DROP ROLE hydradb_test_grantee")
    print("a role granted create_graph, refresh_graph and drop_graph may use them only over tables it owns")


def test_cached_definitions(admin):
    """A backend keeps an indexed graph's catalog description between calls;
    every change that decides whether the adjacency tables may be read, made by
    another backend after the description was cached, still takes effect."""
    for prefix in ("cached", "recached"):
        create_tables(admin, f"hydradb_test.{prefix}_vertices", f"hydradb_test.{prefix}_edges")
    admin.run("INSERT INTO hydradb_test.cached_vertices VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    admin.run("INSERT INTO hydradb_test.cached_edges VALUES (1, 1, 2, 'link')")
    admin.run("INSERT INTO hydradb_test.recached_vertices VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    admin.run("INSERT INTO hydradb_test.recached_edges VALUES (1, 1, 3, 'link')")
    admin.run("GRANT SELECT ON hydradb_test.cached_vertices, hydradb_test.cached_edges, "
              "hydradb_test.recached_vertices, hydradb_test.recached_edges TO hydradb_test_reader")
    admin.run(single_table_graph("cached", "hydradb_test.cached_vertices", "hydradb_test.cached_edges"))
    trigger = "pg_hydradb_' || md5('cached') || '_i"
    with Session() as reader:
        reader.run("SET ROLE hydradb_test_reader")
        for _ in range(2):
            require(ids(query(reader, graph="cached")) == [1, 2], "cached graph initial query failed")

        name = admin.run(f"SELECT '{trigger}'")[0]
        # A disabled trigger flags the graph, and re-enabling it does not
        # clear the flag: the writes in between were never mirrored.
        admin.run(f'ALTER TABLE hydradb_test.cached_edges DISABLE TRIGGER "{name}"')
        disabled = "a pg_hydradb trigger on hydradb_test.cached_edges stopped firing for every write"
        reader.run(query_sql(graph="cached"), error=disabled)
        admin.run("INSERT INTO hydradb_test.cached_edges VALUES (2, 2, 3, 'link')")
        admin.run(f'ALTER TABLE hydradb_test.cached_edges ENABLE ALWAYS TRIGGER "{name}"')
        reader.run(query_sql(graph="cached"), error=disabled)
        admin.run("SELECT hydradb.refresh_graph('cached')")
        require(ids(query(reader, graph="cached", options={"max_depth": 5})) == [1, 2, 3], "refresh missed the unmirrored write")
        admin.run("DELETE FROM hydradb_test.cached_edges WHERE id = 2")
        # A trigger enabled only for origin or only for replica writes skips
        # the others, so leaving always flags the graph as disabling does.
        for mode in ("ENABLE TRIGGER", "ENABLE REPLICA TRIGGER"):
            admin.run(f'ALTER TABLE hydradb_test.cached_edges {mode} "{name}"')
            reader.run(query_sql(graph="cached"), error=disabled)
            admin.run(f'ALTER TABLE hydradb_test.cached_edges ENABLE ALWAYS TRIGGER "{name}"')
            reader.run(query_sql(graph="cached"), error=disabled)
            admin.run("SELECT hydradb.refresh_graph('cached')")
            require(ids(query(reader, graph="cached")) == [1, 2], f"refresh after {mode} did not restore the graph")

        admin.run("ALTER TABLE hydradb_test.cached_vertices ENABLE ROW LEVEL SECURITY")
        reader.run(query_sql(graph="cached"), error="row-level security on cached_vertices applies to it")
        admin.run("ALTER TABLE hydradb_test.cached_vertices DISABLE ROW LEVEL SECURITY")
        require(ids(query(reader, graph="cached")) == [1, 2], "disabled RLS did not restore adjacency reads")

        # Joining an inheritance tree, as a parent or a child, is refused:
        # writes through a parent, and a child's rows, would fire no trigger
        # of the source. So is making it unlogged, which a crash empties
        # without firing its triggers.
        inherited = "hydradb_test.cached_vertices is part of an inheritance tree"
        admin.run("CREATE TABLE hydradb_test.cached_child () INHERITS (hydradb_test.cached_vertices)", error=inherited)
        admin.run("CREATE TABLE hydradb_test.cached_adopter (id bigint NOT NULL, sort_key text NOT NULL)")
        admin.run("ALTER TABLE hydradb_test.cached_vertices INHERIT hydradb_test.cached_adopter", error=inherited)
        require(admin.run("SELECT relhassubclass FROM pg_class WHERE oid = 'hydradb_test.cached_vertices'::regclass")
                == ["f"], "a refused change took effect")
        # PostgreSQL refuses an unlogged table that a logged one's foreign key
        # references, so a table with no edges shows pg_hydradb's own refusal.
        admin.run("CREATE TABLE hydradb_test.cached_lonely (id bigint PRIMARY KEY, sort_key text NOT NULL)")
        admin.run(vertex_graph("cached_lonely", "hydradb_test.cached_lonely"))
        admin.run("ALTER TABLE hydradb_test.cached_lonely SET UNLOGGED", error="hydradb_test.cached_lonely is not a permanent table")
        admin.run("SELECT hydradb.drop_graph('cached_lonely')")
        require(ids(query(reader, graph="cached")) == [1, 2], "a refused change stopped the graph")

        admin.run("SELECT hydradb.drop_graph('cached')")
        admin.run(single_table_graph("cached", "hydradb_test.recached_vertices", "hydradb_test.recached_edges"))
        require(ids(query(reader, graph="cached")) == [1, 3], "re-registered graph answered from the old tables")

        # A registration rolled back in the caller's own transaction is not kept.
        admin.run("BEGIN")
        admin.run("SELECT hydradb.drop_graph('cached')")
        admin.run(single_table_graph("cached", "hydradb_test.cached_vertices", "hydradb_test.cached_edges"))
        require(ids(query(admin, graph="cached")) == [1, 2], "graph registered in a transaction was not visible to it")
        admin.run("ROLLBACK")
        require(ids(query(admin, graph="cached")) == [1, 3], "rolled-back registration was kept")
        require(ids(query(reader, graph="cached")) == [1, 3], "reader lost the committed registration")
    admin.run("SELECT hydradb.drop_graph('cached')")
    print("a cached graph description follows disabled triggers, row-level security, re-registration and rollback; "
          "inheritance and unlogged sources are refused")


def test_registration_race(admin):
    admin.run("CREATE TABLE hydradb_test.late_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.late_edges (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.late_vertices, target bigint NOT NULL REFERENCES hydradb_test.late_vertices, kind text NOT NULL)")
    admin.run("INSERT INTO hydradb_test.late_vertices VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    admin.run("INSERT INTO hydradb_test.late_edges VALUES (1, 1, 2, 'link')")
    with Session() as old_writer, Session() as reader:
        old_writer.run("BEGIN ISOLATION LEVEL REPEATABLE READ")
        require(old_writer.run("SELECT count(*) FROM hydradb_test.late_edges") == ["1"], "failed to pin old writer snapshot")
        admin.run(single_table_graph("late", "hydradb_test.late_vertices", "hydradb_test.late_edges"))
        require(ids(query(reader, graph="late")) == [1, 2], "late graph initial load failed")
        old_writer.run("INSERT INTO hydradb_test.late_edges VALUES (2, 2, 3, 'link')", error="40001")
        old_writer.run("ROLLBACK")
        require(ids(query(reader, graph="late")) == [1, 2], "failed old-snapshot write leaked")
        old_writer.run("INSERT INTO hydradb_test.late_edges VALUES (2, 2, 3, 'link')")
        require(ids(query(reader, graph="late")) == [1, 2, 3], "retried write was not maintained")
    print("pre-registration repeatable-read writer rejected with serialization failure; the retry is maintained")


def test_cast_sensitive_sources(admin):
    admin.run("CREATE TABLE hydradb_test.date_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.date_edges (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.date_vertices, target bigint NOT NULL REFERENCES hydradb_test.date_vertices, kind date NOT NULL)")
    admin.run("INSERT INTO hydradb_test.date_vertices VALUES (1, 'a'), (2, 'b')")
    admin.run("INSERT INTO hydradb_test.date_edges VALUES (1, 1, 2, '2026-10-21')")
    # A date's text form follows DateStyle, so the adjacency tables could not
    # hold it the same way for every session: the graph is refused.
    admin.run(single_table_graph("dates", "hydradb_test.date_vertices", "hydradb_test.date_edges"),
              error="a declared column has type date, whose text form depends on session settings")
    require(admin.run("SELECT count(*) FROM hydradb.graphs WHERE name = 'dates'") == ["0"], "a refused graph was registered")
    print("a cast-sensitive date source is refused at registration")


def test_seed_order(admin):
    """A limit that cuts the seeds keeps the smallest by sort key. Vertex 1 gets
    the lower vid but sorts after vertex 2."""
    create_tables(admin, "hydradb_test.so_vertices", "hydradb_test.so_edges")
    admin.run(single_table_graph("seed_order", "hydradb_test.so_vertices", "hydradb_test.so_edges"))
    # One statement per vertex, so vids follow insertion order.
    for row in ("(1, 'z')", "(2, 'a')", "(3, 'm')"):
        admin.run(f"INSERT INTO hydradb_test.so_vertices VALUES {row}")
    admin.run("INSERT INTO hydradb_test.so_edges VALUES (1, 1, 3, 'link'), (2, 2, 3, 'link')")
    vids = admin.run("""SELECT string_agg(v.key, ',' ORDER BY v.vid) FROM hydradb.graph_vertices v
        JOIN hydradb.graphs g ON g.registration = v.graph WHERE g.name = 'seed_order'""")
    require(vids == ["1,2,3"], f"vids do not follow insertion order: {vids}")
    one = query(admin, graph="seed_order", seeds=(1, 2), options={"max_vertices": 1})
    require(ids(one) == [2] and one["truncated"] == ["vertices"], one)
    # A vertex that is both a source and a target is charged once.
    create_tables(admin, "hydradb_test.one_vertex", "hydradb_test.one_vertex_edges")
    admin.run("INSERT INTO hydradb_test.one_vertex VALUES (1, 'a')")
    admin.run(single_table_graph("one_vertex", "hydradb_test.one_vertex", "hydradb_test.one_vertex_edges"))
    by_key(admin, "paths_between", {"vertex": [1]}, {"vertex": [1]}, graph="one_vertex",
           options={"max_snapshot_bytes": 128 + 8 + 1})
    print("a limit that cuts the seeds keeps the smallest by sort key; "
          "a vertex that is both source and target is charged once")


def test_update_comparison(admin):
    """An UPDATE's old and new rows are compared by their text forms bytewise:
    a case-insensitive collation does not hide a change of case, and one column
    may fill two roles."""
    admin.run("CREATE TABLE hydradb_test.uc_vertices (id bigint PRIMARY KEY)")
    admin.run("INSERT INTO hydradb_test.uc_vertices VALUES (1), (2)")
    admin.run("""SELECT hydradb.create_graph('same_column',
        '[{"label": "vertex", "table": "hydradb_test.uc_vertices", "key": "id", "sort_key": "id"}]')""")
    admin.run("UPDATE hydradb_test.uc_vertices SET id = 3 WHERE id = 2")
    require(admin.run("""SELECT string_agg(v.key, ',' ORDER BY v.key) FROM hydradb.graph_vertices v
        JOIN hydradb.graphs g ON g.registration = v.graph WHERE g.name = 'same_column' AND v.rows > 0""") == ["1,3"],
            "an update of a column declared twice was not mirrored")
    if admin.run("SELECT count(*) FROM pg_collation WHERE collprovider = 'i'") == ["0"]:
        print("one column in two roles is maintained; no ICU collations, case-insensitive kind skipped")
        return
    admin.run("CREATE COLLATION hydradb_test.ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)")
    create_tables(admin, "hydradb_test.uc_kind_vertices", "hydradb_test.uc_kind_edges")
    admin.run("ALTER TABLE hydradb_test.uc_kind_edges ALTER COLUMN kind TYPE text COLLATE hydradb_test.ci")
    admin.run("INSERT INTO hydradb_test.uc_kind_vertices VALUES (1, 'a'), (2, 'b')")
    admin.run("INSERT INTO hydradb_test.uc_kind_edges VALUES (1, 1, 2, 'link')")
    admin.run(single_table_graph("case_kind", "hydradb_test.uc_kind_vertices", "hydradb_test.uc_kind_edges"))
    admin.run("UPDATE hydradb_test.uc_kind_edges SET kind = 'LINK' WHERE id = 1")
    upper = query(admin, graph="case_kind", options={"edge_kind": "LINK"})
    require(ids(upper) == [1, 2], upper)
    require(ids(query(admin, graph="case_kind", options={"edge_kind": "link"})) == [1], "the old kind is still in the graph")
    print("an UPDATE is compared bytewise: a case-insensitive kind's change of case and one column in two roles are maintained")


def test_path_work_limit(admin):
    """Loading a path search's region stops at max_edge_visits with the search's
    own error, rather than loading the region first."""
    create_tables(admin, "hydradb_test.pw_vertices", "hydradb_test.pw_edges")
    admin.run("INSERT INTO hydradb_test.pw_vertices SELECT n, lpad(n::text, 6, '0') FROM generate_series(0, 10010) n")
    # Vertex 0 links to 1..10, and each of those to 1,000 more.
    admin.run("INSERT INTO hydradb_test.pw_edges SELECT n, 0, n, 'link' FROM generate_series(1, 10) n")
    admin.run("INSERT INTO hydradb_test.pw_edges SELECT 10 + n, 1 + (n - 1) / 1000, 10 + n, 'link' FROM generate_series(1, 10000) n")
    admin.run(single_table_graph("path_work", "hydradb_test.pw_vertices", "hydradb_test.pw_edges"))
    work = {"edge_kinds": ["link"], "max_depth": 3, "max_edge_visits": 5}
    by_key(admin, "paths_from", {"vertex": 0}, options=work, graph="path_work",
           error="native_path_edges limit exceeded: 6 > 5")
    # The first layer exceeds the work limit, so the second, 10,000 edges and
    # far larger than this byte budget, is never loaded.
    by_key(admin, "paths_from", {"vertex": 0}, options={**work, "max_snapshot_bytes": 100_000},
           graph="path_work", error="native_path_edges limit exceeded: 6 > 5")
    # A frontier's lookup stops at the work limit too: vertex 1's 1,000 links
    # would exceed this byte budget, but the search would stop at the sixth,
    # and loading stops there with its error.
    for direction in ("outgoing", "both"):
        by_key(admin, "paths_from", {"vertex": 1}, graph="path_work",
               options={**work, "direction": direction, "max_snapshot_bytes": 20_000},
               error="native_path_edges limit exceeded: 6 > 5")
    # A repeated target is walked once, as native_paths walks it. On 1 -> 2 at
    # max_depth 1 the search examines 1 relationship (the target's; with targets
    # it does not walk out from the sources), however often 2 is listed.
    create_tables(admin, "hydradb_test.rt_vertices", "hydradb_test.rt_edges")
    admin.run("INSERT INTO hydradb_test.rt_vertices VALUES (1, 'a'), (2, 'b')")
    admin.run("INSERT INTO hydradb_test.rt_edges VALUES (1, 1, 2, 'link')")
    admin.run(single_table_graph("repeat_targets", "hydradb_test.rt_vertices", "hydradb_test.rt_edges"))
    repeated = {"edge_kinds": ["link"], "max_depth": 1, "max_edge_visits": 1}
    paths = by_key(admin, "paths_between", {"vertex": [1]}, {"vertex": [2, 2, 2]}, options=repeated, graph="repeat_targets")
    require(len(paths["items"]) == 1, paths)
    # Invalid options are refused before any loading.
    by_key(admin, "paths_from", {"vertex": 0}, options={"direction": "sideways"}, graph="path_work",
           error="direction must be")
    print("a path search's region loading stops at max_edge_visits with the search's own error")


def test_incoming_path_hub(admin):
    """A path's relationships come from the lists the search scanned, so an
    incoming walk does not load the outgoing links of the vertices it reaches;
    and with a target it loads only the walk back from the target."""
    create_tables(admin, "hydradb_test.ih_vertices", "hydradb_test.ih_edges")
    admin.run("INSERT INTO hydradb_test.ih_vertices SELECT n, lpad(n::text, 6, '0') FROM generate_series(1, 2100) n")
    # Two parallel links 2 -> 1 and one 3 -> 1; vertex 2 also mentions 2,000 others.
    admin.run("INSERT INTO hydradb_test.ih_edges VALUES (1, 2, 1, 'link'), (2, 2, 1, 'link'), (3, 3, 1, 'link')")
    admin.run("INSERT INTO hydradb_test.ih_edges SELECT 10 + n, 2, 100 + n, 'mention' FROM generate_series(1, 2000) n")
    admin.run(single_table_graph("incoming_hub", "hydradb_test.ih_vertices", "hydradb_test.ih_edges"))
    for direction in ("incoming", "both"):
        options = {"edge_kinds": ["link"], "direction": direction, "max_depth": 1, "paths_per_pair": 0}
        paths = by_key(admin, "paths_from", {"vertex": 1}, options=options, graph="incoming_hub")
        require(len(paths["items"]) == 3, paths)
        # Only vertex 1's lists are read, well under this budget; loading
        # vertex 2's outgoing links as well would exceed it.
        budgeted = by_key(admin, "paths_from", {"vertex": 1}, graph="incoming_hub",
                          options={**options, "max_snapshot_bytes": 20_000})
        require(normalized(budgeted, "paths_from")["items"] == paths["items"], (budgeted, paths))
    # From hub 2 to vertex 1, outgoing: the walk back from 1 reads 1's three
    # incoming links. Walking out from 2 as well would read its 2,002 links.
    options = {"edge_kinds": ["link"], "max_depth": 2, "paths_per_pair": 0}
    paths = by_key(admin, "paths_between", {"vertex": 2}, {"vertex": 1}, options=options, graph="incoming_hub")
    require(len(paths["items"]) == 2, paths)
    budgeted = by_key(admin, "paths_between", {"vertex": 2}, {"vertex": 1}, graph="incoming_hub",
                      options={**options, "max_snapshot_bytes": 20_000})
    require(normalized(budgeted, "paths_between")["items"] == paths["items"], (budgeted, paths))
    print("a path walk takes its relationships from the lists it scanned, and with a target scans only back from it")


def test_column_rewrite_flagged(admin):
    """ALTER TABLE that rewrites column values bypasses triggers, so on a table
    a graph reads it stops the graph's queries until refresh_graph rebuilds it
    from the new values; other rewrites and tables are left alone."""
    create_tables(admin, "hydradb_test.rw_vertices", "hydradb_test.rw_edges")
    admin.run("INSERT INTO hydradb_test.rw_vertices VALUES (1, 'a'), (2, 'b')")
    admin.run("INSERT INTO hydradb_test.rw_edges VALUES (1, 1, 2, 'link')")
    admin.run(single_table_graph("rewrites", "hydradb_test.rw_vertices", "hydradb_test.rw_edges"))
    flagged = lambda: admin.run("SELECT needs_refresh IS NOT NULL FROM hydradb.graphs WHERE name = 'rewrites'") == ["t"]
    stopped = "graph rewrites cannot be queried until hydradb.refresh_graph('rewrites')"
    for table, rewrite in (("rw_edges", "ALTER COLUMN kind TYPE text USING upper(kind)"),
                           ("rw_vertices", "ALTER COLUMN sort_key TYPE text USING upper(sort_key)")):
        warned = admin.run(f"ALTER TABLE hydradb_test.{table} {rewrite}")
        require(any(f"graph rewrites: an ALTER TABLE rewrote the column values of hydradb_test.{table}" in line
                    for line in warned), warned)
        require(flagged(), f"a rewrite of {table} did not flag the graph")
        admin.run(query_sql(graph="rewrites"), error=stopped)
        admin.run("SELECT hydradb.refresh_graph('rewrites')")
        require(not flagged(), f"refresh after a rewrite of {table} left the flag")
        query(admin, graph="rewrites")
        adjacency_matches_sources(admin, "rewrites")
    # The refreshed graph reads the new kind.
    require(ids(query(admin, graph="rewrites", options={"edge_kind": "LINK"})) == [1, 2], "the refreshed graph missed the new kind")
    # A rewrite that changes a declared column's type is refused, as any
    # change to a declared column is, and leaves the graph as it was.
    admin.run("ALTER TABLE hydradb_test.rw_vertices ALTER COLUMN sort_key TYPE varchar(10)",
              error="cannot change column sort_key of hydradb_test.rw_vertices: it is the sort key of indexed graph rewrites")
    require(not flagged(), "a refused retype flagged the graph")
    # Rewrites that keep existing values, and tables no indexed graph reads, are left alone.
    admin.run("ALTER TABLE hydradb_test.rw_edges ADD COLUMN note text DEFAULT clock_timestamp()::text")
    admin.run("ALTER TABLE hydradb_test.rw_edges ALTER COLUMN note TYPE varchar")
    require(not flagged(), "a rewrite that keeps existing values flagged the graph")
    admin.run("CREATE TABLE hydradb_test.rw_other (id bigint, label text)")
    admin.run("ALTER TABLE hydradb_test.rw_other ALTER COLUMN label TYPE text USING upper(label)")
    # The event trigger runs for every role's rewrites, and lets them through.
    admin.run("CREATE ROLE hydradb_test_rewriter")
    admin.run("GRANT USAGE, CREATE ON SCHEMA hydradb_test TO hydradb_test_rewriter")
    with Session() as rewriter:
        rewriter.run("SET SESSION AUTHORIZATION hydradb_test_rewriter")
        rewriter.run("CREATE TABLE hydradb_test.rw_owned (label text)")
        rewriter.run("ALTER TABLE hydradb_test.rw_owned ALTER COLUMN label TYPE text USING upper(label)")
        rewriter.run("DROP TABLE hydradb_test.rw_owned")
    admin.run("REVOKE USAGE, CREATE ON SCHEMA hydradb_test FROM hydradb_test_rewriter")
    admin.run("DROP ROLE hydradb_test_rewriter")
    # A source owned by another role flags the graph when that role rewrites it.
    admin.run("CREATE ROLE hydradb_test_rewriter")
    admin.run("GRANT USAGE ON SCHEMA hydradb_test TO hydradb_test_rewriter")
    admin.run("ALTER TABLE hydradb_test.rw_edges OWNER TO hydradb_test_rewriter")
    with Session() as rewriter:
        rewriter.run("SET SESSION AUTHORIZATION hydradb_test_rewriter")
        rewriter.run("ALTER TABLE hydradb_test.rw_edges ALTER COLUMN kind TYPE text USING lower(kind)")
    require(flagged(), "a rewrite by the table's owner did not flag the graph")
    admin.run("ALTER TABLE hydradb_test.rw_edges OWNER TO hydradb_test_admin")
    admin.run("REVOKE USAGE ON SCHEMA hydradb_test FROM hydradb_test_rewriter")
    admin.run("DROP ROLE hydradb_test_rewriter")
    admin.run("SELECT hydradb.refresh_graph('rewrites')")
    require(ids(query(admin, graph="rewrites", options={"edge_kind": "link"})) == [1, 2], "the refreshed graph missed the new kind")
    # An indexed graph cannot declare a stored generated column, which SET
    # EXPRESSION (PostgreSQL 17) would recompute in a rewrite that fires no
    # triggers. So a rewrite for a default only ever adds a column, and passes
    # even on a table that has a generated column the graph does not declare.
    admin.run("CREATE TABLE hydradb_test.gen_edges (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.rw_vertices, "
              "target bigint NOT NULL REFERENCES hydradb_test.rw_vertices, "
              "raw_kind text NOT NULL, kind text NOT NULL GENERATED ALWAYS AS (lower(raw_kind)) STORED)")
    admin.run("INSERT INTO hydradb_test.gen_edges (id, source, target, raw_kind) VALUES (1, 1, 2, 'Link')")
    admin.run(single_table_graph("generated", "hydradb_test.rw_vertices", "hydradb_test.gen_edges"),
              error="column kind of hydradb_test.gen_edges is a declared kind but a generated column")
    require(admin.run("SELECT count(*) FROM hydradb.graphs WHERE name = 'generated'") == ["0"], "a refused graph was registered")
    admin.run("ALTER TABLE hydradb_test.gen_edges RENAME COLUMN kind TO derived_kind")
    admin.run("ALTER TABLE hydradb_test.gen_edges ADD COLUMN kind text NOT NULL DEFAULT 'link'")
    admin.run(single_table_graph("generated", "hydradb_test.rw_vertices", "hydradb_test.gen_edges"))
    admin.run("ALTER TABLE hydradb_test.gen_edges ADD COLUMN note text DEFAULT random()::text")
    require(ids(query(admin, graph="generated", options={"edge_kind": "link"})) == [1, 2],
            "a rewrite for an added column stopped the graph")
    # Swapping two declared names takes three renames, one ALTER TABLE each
    # (PostgreSQL allows one RENAME per statement), and the first, which
    # leaves a declared name missing, is refused.
    create_tables(admin, "hydradb_test.sw_vertices", "hydradb_test.sw_edges")
    admin.run("INSERT INTO hydradb_test.sw_vertices VALUES (1, 'a'), (2, 'b')")
    admin.run("INSERT INTO hydradb_test.sw_edges VALUES (1, 1, 2, 'link')")
    admin.run(single_table_graph("swaps", "hydradb_test.sw_vertices", "hydradb_test.sw_edges"))
    admin.run("ALTER TABLE hydradb_test.sw_edges RENAME COLUMN source TO tmp, RENAME COLUMN target TO source",
              error="syntax error")
    admin.run("ALTER TABLE hydradb_test.sw_edges RENAME COLUMN source TO tmp",
              error="cannot rename column source of hydradb_test.sw_edges: it is the source of indexed graph swaps")
    adjacency_matches_sources(admin, "swaps")
    require(ids(query(admin, graph="swaps", seeds=(1,))) == [1, 2], "a refused rename changed the graph")
    print("a column rewrite of a graph's table stops its queries until refresh_graph; retyping or renaming a declared "
          "column is refused; a graph declaring a generated column is refused; other rewrites pass")


def test_isolation_and_columns(admin):
    """create_graph and refresh_graph fill from what they read after locking,
    and drop_graph checks ownership after locking, so they need READ COMMITTED; a repeatable-read writer whose snapshot
    predates a refresh retries; dropping a declared column or a source table
    is refused, and so is renaming a declared column."""
    create_tables(admin, "hydradb_test.iso_vertices", "hydradb_test.iso_edges")
    admin.run("INSERT INTO hydradb_test.iso_vertices VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    admin.run("INSERT INTO hydradb_test.iso_edges VALUES (1, 1, 2, 'link')")
    with Session() as session:
        for level in ("REPEATABLE READ", "SERIALIZABLE"):
            session.run(f"BEGIN ISOLATION LEVEL {level}")
            session.run(single_table_graph("isolated", "hydradb_test.iso_vertices", "hydradb_test.iso_edges"),
                        error=f"create_graph needs a READ COMMITTED transaction, not {level}")
            session.run("ROLLBACK")
    admin.run(single_table_graph("isolated", "hydradb_test.iso_vertices", "hydradb_test.iso_edges"))
    with Session() as session:
        for operation in ("refresh_graph", "drop_graph"):
            session.run("BEGIN ISOLATION LEVEL REPEATABLE READ")
            session.run(f"SELECT hydradb.{operation}('isolated')", error=f"{operation} needs a READ COMMITTED transaction")
            session.run("ROLLBACK")
    # A writer whose snapshot predates a refresh would mirror into vertex ids
    # the refresh replaced; it fails to retry instead.
    with Session() as old_writer:
        old_writer.run("BEGIN ISOLATION LEVEL REPEATABLE READ")
        old_writer.run("SELECT count(*) FROM hydradb_test.iso_edges")
        admin.run("SELECT hydradb.refresh_graph('isolated')")
        old_writer.run("INSERT INTO hydradb_test.iso_edges VALUES (2, 2, 3, 'link')", error="40001")
        old_writer.run("ROLLBACK")
        old_writer.run("INSERT INTO hydradb_test.iso_edges VALUES (2, 2, 3, 'link')")
    adjacency_matches_sources(admin, "isolated")
    readable = lambda: query(admin, graph="isolated") is not None
    require(readable(), "the refreshed graph cannot be queried")
    # Dropping a declared column is refused, even when the same ALTER adds a
    # column of that name back, whose values the triggers never saw; so is
    # dropping a source table, directly or with its schema.
    refused = "cannot drop column kind of hydradb_test.iso_edges: indexed graph isolated declares it"
    lines = admin.run("ALTER TABLE hydradb_test.iso_edges DROP COLUMN kind", error=refused)
    require(any("hydradb.drop_graph" in line for line in lines), lines)
    admin.run("ALTER TABLE hydradb_test.iso_edges DROP COLUMN kind, ADD COLUMN kind text NOT NULL DEFAULT 'other'", error=refused)
    admin.run("DROP TABLE hydradb_test.iso_vertices CASCADE",
              error="cannot drop table hydradb_test.iso_vertices: it is a source of indexed graph isolated")
    require(readable(), "a refused drop flagged the graph")
    require(ids(query(admin, graph="isolated", options={"edge_kind": "link"})) == [1, 2, 3], "a refused drop changed the graph")
    adjacency_matches_sources(admin, "isolated")
    # An undeclared column changes nothing; renaming a declared one is refused.
    admin.run("ALTER TABLE hydradb_test.iso_edges ADD COLUMN note text")
    admin.run("ALTER TABLE hydradb_test.iso_edges DROP COLUMN note")
    require(readable(), "an undeclared column flagged the graph")
    lines = admin.run("ALTER TABLE hydradb_test.iso_vertices RENAME COLUMN sort_key TO ordering",
                      error="cannot rename column sort_key of hydradb_test.iso_vertices: it is the sort key of indexed graph isolated")
    require(any("hydradb.drop_graph" in line for line in lines), lines)
    require(readable(), "a refused rename stopped the graph")
    admin.run("SELECT hydradb.drop_graph('isolated')")
    # The way round a refusal: with the graph dropped, the drop succeeds.
    admin.run("ALTER TABLE hydradb_test.iso_edges DROP COLUMN kind")
    print("create_graph, refresh_graph and drop_graph need READ COMMITTED; a repeatable-read writer older than a refresh retries; "
          "dropping a declared column or source, or renaming a declared column, is refused")


def test_declared_columns_fixed(admin):
    """While a graph exists, an ALTER TABLE may change anything about its
    tables except what the graph depends on: whatever the command, one that
    leaves a declared column different from registration (type, modifier,
    collation, nullability, name) is refused, as is renaming a pg_hydradb
    trigger. A change made with the event triggers off stops refresh_graph
    until it is undone."""
    create_tables(admin, "hydradb_test.fx_vertices", "hydradb_test.fx_edges")
    admin.run("INSERT INTO hydradb_test.fx_vertices VALUES (1, 'a'), (2, 'b')")
    admin.run("INSERT INTO hydradb_test.fx_edges VALUES (1, 1, 2, 'link')")
    admin.run(single_table_graph("fixed", "hydradb_test.fx_vertices", "hydradb_test.fx_edges"))
    flagged = lambda: admin.run("SELECT needs_refresh IS NOT NULL FROM hydradb.graphs WHERE name = 'fixed'") == ["t"]
    healthy = lambda: (not flagged(), ids(query(admin, graph="fixed")) == [1, 2])
    for change in ("ADD COLUMN note text",
                   "ALTER COLUMN note TYPE varchar",
                   "ADD CHECK (octet_length(kind) <= 100)",
                   "ALTER COLUMN kind SET STATISTICS 500",
                   "ALTER COLUMN kind SET STORAGE EXTERNAL",
                   "SET (fillfactor = 90)",
                   "DROP COLUMN note"):
        admin.run(f"ALTER TABLE hydradb_test.fx_edges {change}")
        require(healthy() == (True, True), f"{change} stopped the graph")
    admin.run("CREATE INDEX fx_kind ON hydradb_test.fx_edges (kind)")
    admin.run("COMMENT ON COLUMN hydradb_test.fx_edges.kind IS 'relationship type'")
    for change, role, column in (("ALTER COLUMN kind TYPE varchar(20)", "kind", "kind"),
                                 ("ALTER COLUMN source TYPE integer", "source", "source"),
                                 ("ALTER COLUMN target DROP NOT NULL", "target", "target"),
                                 ("RENAME COLUMN id TO edge_id", "key", "id")):
        verb = "rename" if "RENAME" in change else "change"
        admin.run(f"ALTER TABLE hydradb_test.fx_edges {change}",
                  error=f"cannot {verb} column {column} of hydradb_test.fx_edges: it is the {role} of indexed graph fixed")
        require(healthy() == (True, True), f"refused {change} stopped the graph")
    trigger = admin.run("SELECT 'pg_hydradb_' || md5('fixed') || '_i'")[0]
    admin.run(f'ALTER TRIGGER "{trigger}" ON hydradb_test.fx_edges RENAME TO renamed',
              error="cannot alter a pg_hydradb trigger on hydradb_test.fx_edges: it keeps the adjacency tables of graph fixed current")
    admin.run("ALTER TABLE hydradb_test.fx_edges RENAME TO fx_links")
    require(healthy() == (True, True), "renaming a source table stopped the graph")
    admin.run("ALTER TABLE hydradb_test.fx_links RENAME TO fx_edges")
    # With the event triggers off nothing refuses the rename, and a refresh
    # then refuses until the name is back.
    admin.run("ALTER EVENT TRIGGER pg_hydradb_check_altered_sources DISABLE")
    admin.run("ALTER TABLE hydradb_test.fx_vertices RENAME COLUMN sort_key TO ordering")
    admin.run("ALTER EVENT TRIGGER pg_hydradb_check_altered_sources ENABLE")
    admin.run("SELECT hydradb.refresh_graph('fixed')",
              error="column sort_key of hydradb_test.fx_vertices, the graph's sort key, was text COLLATE "
                    'pg_catalog."default" NOT NULL at registration and is now missing')
    admin.run("ALTER EVENT TRIGGER pg_hydradb_check_altered_sources DISABLE")
    admin.run("ALTER TABLE hydradb_test.fx_vertices RENAME COLUMN ordering TO sort_key")
    admin.run("ALTER EVENT TRIGGER pg_hydradb_check_altered_sources ENABLE")
    admin.run("SELECT hydradb.refresh_graph('fixed')")
    adjacency_matches_sources(admin, "fixed")
    # A collation, or its schema, renamed is the same collation: later ALTERs
    # and refreshes pass. Moving the column to another collation is refused.
    admin.run("CREATE SCHEMA hydradb_test_coll")
    admin.run("CREATE COLLATION hydradb_test_coll.bytes (provider = libc, locale = 'C')")
    create_tables(admin, "hydradb_test.co_vertices", "hydradb_test.co_edges")
    admin.run("ALTER TABLE hydradb_test.co_edges ALTER COLUMN kind TYPE text COLLATE hydradb_test_coll.bytes")
    admin.run("INSERT INTO hydradb_test.co_vertices VALUES (1, 'a'), (2, 'b')")
    admin.run("INSERT INTO hydradb_test.co_edges VALUES (1, 1, 2, 'link')")
    admin.run(single_table_graph("collated", "hydradb_test.co_vertices", "hydradb_test.co_edges"))
    admin.run("ALTER COLLATION hydradb_test_coll.bytes RENAME TO raw_bytes")
    admin.run("ALTER SCHEMA hydradb_test_coll RENAME TO hydradb_test_collations")
    admin.run("ALTER TABLE hydradb_test.co_edges ADD COLUMN note text")
    admin.run("SELECT hydradb.refresh_graph('collated')")
    require(admin.run("SELECT state -> 'kind' ->> 'collation' FROM hydradb.graph_edge_tables WHERE graph = 'collated'")
            == ["hydradb_test_collations.raw_bytes"], "the recorded collation name did not follow the renames")
    admin.run('ALTER TABLE hydradb_test.co_edges ALTER COLUMN kind TYPE text COLLATE "C"',
              error="cannot change column kind of hydradb_test.co_edges: it is the kind of indexed graph collated")
    require(ids(query(admin, graph="collated")) == [1, 2], "a renamed collation stopped the graph")
    # Dropped, the graph no longer holds its columns.
    admin.run("SELECT hydradb.drop_graph('fixed')")
    admin.run("ALTER TABLE hydradb_test.fx_edges ALTER COLUMN kind TYPE varchar(20)")
    admin.run("ALTER TABLE hydradb_test.fx_edges RENAME COLUMN id TO edge_id")
    print("an ALTER that changes a declared column, or renames a pg_hydradb trigger, is refused whatever the command; "
          "other changes pass; refresh_graph refuses a change made with the event triggers off")


def test_long_keys(admin):
    """A key the graph's indexes cannot hold never fails the write that brings
    it: the graph stops trusting its adjacency tables, says why, and its
    queries fail with that reason until refresh_graph."""
    admin.run("CREATE TABLE hydradb_test.longkey_vertices (id text PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.longkey_edges (id bigint PRIMARY KEY, source text NOT NULL REFERENCES hydradb_test.longkey_vertices, target text NOT NULL REFERENCES hydradb_test.longkey_vertices, kind text NOT NULL)")
    admin.run("INSERT INTO hydradb_test.longkey_vertices VALUES ('a', 'a'), ('b', 'b')")
    admin.run("INSERT INTO hydradb_test.longkey_edges VALUES (1, 'a', 'b', 'link')")
    admin.run(single_table_graph("long_keys", "hydradb_test.longkey_vertices", "hydradb_test.longkey_edges"))
    seed, keys = {"vertex": ["a"]}, lambda result: [item["key"] for item in result["items"]]
    flagged = "SELECT needs_refresh IS NOT NULL FROM hydradb.graphs WHERE name = 'long_keys'"
    stopped = ("graph long_keys cannot be queried until hydradb.refresh_graph('long_keys') rebuilds its adjacency tables: "
               "column id of hydradb_test.longkey_vertices has a value of 4000 bytes")
    require(keys(by_key(admin, "reachable_vertices", seed, graph="long_keys")) == ["a", "b"], "initial query")
    # 4000 bytes of hex does not fit an index entry: the write used to fail. A
    # vertex key needs a unique index of its own, which rejects that value
    # itself, so vertex rows use 4000 bytes that compress into one.
    long = "(SELECT string_agg(md5(i::text), '') FROM generate_series(1, 125) i)"
    long_vertex = "repeat('ab', 2000)"
    with Session() as reader:
        require(keys(by_key(reader, "reachable_vertices", seed, graph="long_keys")) == ["a", "b"], "reader's initial query")
        warned = admin.run(f"INSERT INTO hydradb_test.longkey_vertices SELECT {long_vertex}, 'z'")
        require(any("column id of hydradb_test.longkey_vertices has a value of 4000 bytes" in line for line in warned), warned)
        require(any("fail until hydradb.refresh_graph('long_keys')" in line for line in warned), warned)
        require(admin.run(flagged) == ["t"], "the graph was not flagged")
        # A backend that cached the graph notices and fails with the reason.
        by_key(reader, "reachable_vertices", seed, graph="long_keys", error=stopped)
    by_key(admin, "reachable_vertices", seed, graph="long_keys", error=stopped)
    # Writes go on while it needs refresh, and the refresh sees them.
    admin.run("INSERT INTO hydradb_test.longkey_vertices VALUES ('c', 'c')")
    admin.run("INSERT INTO hydradb_test.longkey_edges VALUES (2, 'b', 'c', 'link')")
    admin.run("SELECT hydradb.refresh_graph('long_keys')", error="has a value of 4000 bytes")
    admin.run("DELETE FROM hydradb_test.longkey_vertices WHERE octet_length(id) > 2000")
    admin.run("SELECT hydradb.refresh_graph('long_keys')")
    require(admin.run(flagged) == ["f"], "refresh_graph left the flag")
    refreshed = by_key(admin, "reachable_vertices", seed, graph="long_keys")
    require(keys(refreshed) == ["a", "b", "c"], refreshed)
    adjacency_matches_sources(admin, "long_keys")
    # An edge endpoint is a vertex key too.
    # The foreign key would need the long vertex; a replica-role write skips it.
    warned = admin.run(f"SET session_replication_role = replica; INSERT INTO hydradb_test.longkey_edges SELECT 3, 'a', {long}, 'link'; "
                       "RESET session_replication_role")
    require(any("column target of hydradb_test.longkey_edges has a value of 4000 bytes" in line for line in warned), warned)
    admin.run("DELETE FROM hydradb_test.longkey_edges WHERE id = 3")
    admin.run("SELECT hydradb.refresh_graph('long_keys')")
    # Registering over a table that already holds one is refused.
    admin.run("SELECT hydradb.drop_graph('long_keys')")
    admin.run(f"INSERT INTO hydradb_test.longkey_vertices SELECT {long_vertex}, 'z'")
    admin.run(single_table_graph("long_keys", "hydradb_test.longkey_vertices", "hydradb_test.longkey_edges"),
              error="column id of hydradb_test.longkey_vertices has a value of 4000 bytes")
    require(admin.run("SELECT count(*) FROM hydradb.graphs WHERE name = 'long_keys'") == ["0"], "a refused graph was registered")
    admin.run("DELETE FROM hydradb_test.longkey_vertices WHERE octet_length(id) > 2000")
    admin.run(single_table_graph("long_keys", "hydradb_test.longkey_vertices", "hydradb_test.longkey_edges"))
    adjacency_matches_sources(admin, "long_keys")
    # Repeatable-read writers lock the graph row only against a key change, so
    # one that flags it does not wait for another that merely wrote an edge.
    with Session() as first, Session() as second:
        for session, key in ((first, 10), (second, 11)):
            session.run("BEGIN ISOLATION LEVEL REPEATABLE READ")
            session.run(f"INSERT INTO hydradb_test.longkey_edges VALUES ({key}, 'a', 'b', 'link')")
        first.run(f"INSERT INTO hydradb_test.longkey_vertices SELECT {long_vertex}, 'z'")
        first.run("COMMIT")
        second.run(f"INSERT INTO hydradb_test.longkey_vertices SELECT {long_vertex} || 'x', 'z'", error="40001")
        second.run("ROLLBACK")
    admin.run("DELETE FROM hydradb_test.longkey_vertices WHERE octet_length(id) > 2000")
    admin.run("DELETE FROM hydradb_test.longkey_edges WHERE id = 10")
    admin.run("SELECT hydradb.refresh_graph('long_keys')")
    adjacency_matches_sources(admin, "long_keys")
    print("a key the indexes cannot hold leaves its write alone and stops the graph's queries until refresh_graph; "
          "registering over one is refused")


def test_hub_layer(admin):
    # One vertex with more edges than an adjacency lookup fetches at a time.
    admin.run("CREATE TABLE hydradb_test.hub_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.hub_edges (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.hub_vertices, target bigint NOT NULL REFERENCES hydradb_test.hub_vertices, kind text NOT NULL)")
    admin.run("INSERT INTO hydradb_test.hub_vertices SELECT n, lpad(n::text, 6, '0') FROM generate_series(0, 10000) n")
    admin.run("INSERT INTO hydradb_test.hub_edges SELECT n, 0, n, 'link' FROM generate_series(1, 10000) n")
    admin.run(single_table_graph("hub", "hydradb_test.hub_vertices", "hydradb_test.hub_edges"))
    admin.run("VACUUM ANALYZE hydradb.graph_adjacency")
    whole = query(admin, graph="hub", seeds=(0,), options={"max_depth": 1})
    require(len(whole["items"]) == 10001 and whole["stats"]["edge_visits"] == 10000, whole["stats"])
    # The byte budget stops the hub's layer while it is read, charged as each
    # new edge is read.
    admin.run(query_sql(graph="hub", seeds=(0,), options={"max_depth": 1, "max_snapshot_bytes": 100000}),
              error="snapshot byte budget exceeded")
    print("a hub layer larger than one lookup batch is read whole, and the byte budget stops it")


def test_mirrorable(admin):
    """An indexed graph's triggers never reject a write its tables accept:
    registration refuses nullable declared columns and vertex and edge keys
    without an immediate unique index of their own, and leaves nothing behind."""
    admin.run("CREATE TABLE hydradb_test.m_vertices (id bigint NOT NULL, sort_key text)")
    admin.run("CREATE TABLE hydradb_test.m_edges (id bigint, source bigint NOT NULL, target bigint NOT NULL, kind text)")
    register = single_table_graph("mirrored", "hydradb_test.m_vertices", "hydradb_test.m_edges")
    admin.run(register, error="column sort_key of hydradb_test.m_vertices is a declared sort key but allows NULL")
    admin.run("ALTER TABLE hydradb_test.m_vertices ALTER COLUMN sort_key SET NOT NULL")
    admin.run(register, error="column id of hydradb_test.m_edges is a declared key but allows NULL")
    admin.run("ALTER TABLE hydradb_test.m_edges ALTER COLUMN id SET NOT NULL")
    admin.run(register, error="column kind of hydradb_test.m_edges is a declared kind but allows NULL")
    admin.run("ALTER TABLE hydradb_test.m_edges ALTER COLUMN kind SET NOT NULL")
    # Unique indexes that do not make the key unique on its own, at every write.
    for index in ("CREATE UNIQUE INDEX m_partial ON hydradb_test.m_edges (id) WHERE id > 0",
                  "CREATE UNIQUE INDEX m_pair ON hydradb_test.m_edges (id, source)",
                  "CREATE UNIQUE INDEX m_expression ON hydradb_test.m_edges ((id + 0))",
                  "ALTER TABLE hydradb_test.m_edges ADD CONSTRAINT m_deferred UNIQUE (id) DEFERRABLE"):
        admin.run(index)
        admin.run(register, error="edge key id of hydradb_test.m_edges has no immediate unique index of its own")
    require(admin.run("SELECT count(*) FROM hydradb.graphs WHERE name = 'mirrored'") == ["0"], "a refused graph was registered")
    require(admin.run("SELECT count(*) FROM pg_trigger WHERE tgrelid = 'hydradb_test.m_edges'::regclass AND tgname LIKE 'pg_hydradb_%'") == ["0"],
            "a refused graph left triggers")
    admin.run("ALTER TABLE hydradb_test.m_edges ADD UNIQUE (id)")
    # A vertex key needs one too, or the table would accept a duplicate vertex.
    admin.run(register, error="vertex key id of hydradb_test.m_vertices has no unique index of its own")
    admin.run("ALTER TABLE hydradb_test.m_vertices ADD PRIMARY KEY (id)")
    # Each edge endpoint needs a foreign key to the vertex key it names.
    admin.run(register, error="column source of hydradb_test.m_edges is an edge source with no foreign key to key id of hydradb_test.m_vertices")
    admin.run("ALTER TABLE hydradb_test.m_edges ADD FOREIGN KEY (source) REFERENCES hydradb_test.m_vertices, "
              "ADD FOREIGN KEY (target) REFERENCES hydradb_test.m_vertices")
    admin.run(register)
    require(admin.run("SELECT indexed FROM hydradb.graphs WHERE name = 'mirrored'") == ["t"], "the graph was not indexed")
    admin.run("INSERT INTO hydradb_test.m_vertices VALUES (1, 'a'), (2, 'b')")
    admin.run("INSERT INTO hydradb_test.m_edges VALUES (1, 1, 2, 'link')")
    flagged = lambda: admin.run("SELECT needs_refresh IS NOT NULL FROM hydradb.graphs WHERE name = 'mirrored'") == ["t"]
    # Losing what registration required is refused, since the table would
    # then accept a write the triggers reject: a declared column's NOT NULL,
    # by ALTER COLUMN or, from PostgreSQL 18, by dropping its constraint ...
    nullable = "cannot change column kind of hydradb_test.m_edges: it is the kind of indexed graph mirrored"
    admin.run("ALTER TABLE hydradb_test.m_edges ALTER COLUMN kind DROP NOT NULL", error=nullable)
    if int(admin.run("SHOW server_version_num")[0]) >= 180000:
        constraint = admin.run("SELECT conname FROM pg_constraint WHERE conrelid = 'hydradb_test.m_edges'::regclass AND contype = 'n' "
                               "AND conkey = ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid = conrelid AND attname = 'kind')]")[0]
        admin.run(f'ALTER TABLE hydradb_test.m_edges DROP CONSTRAINT "{constraint}"', error=nullable)
    # ... and the edge key's last immediate unique index, by constraint or by
    # DROP INDEX. Another one keeps the key unique, and the unique indexes the
    # graph does not count may go.
    admin.run("CREATE UNIQUE INDEX m_only ON hydradb_test.m_edges (id)")
    admin.run("ALTER TABLE hydradb_test.m_edges DROP CONSTRAINT m_edges_id_key")
    lastindex = "cannot drop the last immediate unique index of edge key id of hydradb_test.m_edges: indexed graph mirrored needs it"
    admin.run("DROP INDEX hydradb_test.m_only", error=lastindex)
    admin.run("DROP INDEX hydradb_test.m_partial, hydradb_test.m_expression")
    admin.run("ALTER TABLE hydradb_test.m_edges DROP CONSTRAINT m_deferred")
    admin.run("ALTER TABLE hydradb_test.m_edges ADD UNIQUE (id)")
    admin.run("DROP INDEX hydradb_test.m_only")
    admin.run("ALTER TABLE hydradb_test.m_edges DROP CONSTRAINT m_edges_id_key", error=lastindex)
    # A vertex key the edges' foreign keys reference cannot lose its index:
    # PostgreSQL refuses first.
    admin.run("ALTER TABLE hydradb_test.m_vertices DROP CONSTRAINT m_vertices_pkey", error="other objects depend on it")
    require(not flagged() and admin.run("SELECT attnotnull FROM pg_attribute WHERE attrelid = 'hydradb_test.m_edges'::regclass "
                                        "AND attname = 'kind'") == ["t"], "a refused change took effect")
    admin.run("INSERT INTO hydradb_test.m_edges VALUES (2, 2, 1, 'link')")
    adjacency_matches_sources(admin, "mirrored")
    # DROP INDEX CONCURRENTLY commits its first steps, which leave the index
    # unenforced, before sql_drop runs, so it is refused when it starts, read
    # from the statement, and the index stays whole. A statement that cannot
    # be read that way goes through, and the graph is flagged instead, for an
    # edge key as for the key of a vertex table no foreign key references.
    admin.run("CREATE TABLE hydradb_test.m_lonely (id bigint PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("INSERT INTO hydradb_test.m_lonely VALUES (1, 'a')")
    admin.run("""SELECT hydradb.create_graph('lonely', '[{"label": "vertex", "table": "hydradb_test.m_lonely", "key": "id", "sort_key": "sort_key"}]')""")
    for graph, table, constraint, role, restore, duplicate in (
            ("mirrored", "m_edges", "m_edges_id_key", "edge", "ADD UNIQUE (id)", "(1, 2, 1, 'link')"),
            ("lonely", "m_lonely", "m_lonely_pkey", "vertex", "ADD PRIMARY KEY (id)", "(1, 'z')")):
        admin.run(f"CREATE UNIQUE INDEX m_last ON hydradb_test.{table} (id)")
        admin.run(f"ALTER TABLE hydradb_test.{table} DROP CONSTRAINT {constraint}")
        refused = f"cannot drop the last {'immediate ' if role == 'edge' else ''}unique index of {role} key id of hydradb_test.{table}"
        for drop in ("DROP INDEX CONCURRENTLY hydradb_test.m_last",
                     "drop index concurrently if exists hydradb_test.m_last restrict",
                     'DROP INDEX CONCURRENTLY "hydradb_test"."m_last" CASCADE'):
            admin.run(drop, error=refused)
        admin.run("SET search_path = hydradb_test")
        admin.run("DROP INDEX CONCURRENTLY m_last", error=refused)
        admin.run("RESET search_path")
        require(admin.run("SELECT indisvalid AND indisready FROM pg_index WHERE indexrelid = 'hydradb_test.m_last'::regclass") == ["t"],
                "a refused concurrent drop left the index unenforced")
        require(admin.run(f"SELECT needs_refresh IS NULL FROM hydradb.graphs WHERE name = '{graph}'") == ["t"],
                "a refused concurrent drop flagged the graph")
        warned = admin.run("DROP INDEX CONCURRENTLY /* not read */ hydradb_test.m_last")
        lost = f"lost the last {'immediate ' if role == 'edge' else ''}unique index of {role} key id of hydradb_test.{table}"
        require(any(lost in line for line in warned), warned)
        require(admin.run("SELECT count(*) FROM pg_class WHERE relname = 'm_last'") == ["0"], "the index was left half dropped")
        require(lost in admin.run(f"SELECT needs_refresh FROM hydradb.graphs WHERE name = '{graph}'")[0], "the graph was not flagged")
        admin.run(query_sql(graph=graph), error=f"cannot be queried until hydradb.refresh_graph('{graph}')")
        # A flagged graph's triggers mirror nothing, so a duplicate key, which
        # the table now accepts, fails no write; unrelated drops go through.
        admin.run(f"INSERT INTO hydradb_test.{table} VALUES {duplicate}")
        admin.run(f"CREATE INDEX m_plain ON hydradb_test.{table} (id)")
        admin.run("DROP INDEX hydradb_test.m_plain")
        admin.run(f"SELECT hydradb.refresh_graph('{graph}')", error=f"{role} key id of hydradb_test.{table} has no")
        admin.run(f"DELETE FROM hydradb_test.{table} WHERE ({'id, source, target, kind' if role == 'edge' else 'id, sort_key'}) = {duplicate}")
        admin.run(f"ALTER TABLE hydradb_test.{table} {restore}")
        admin.run(f"SELECT hydradb.refresh_graph('{graph}')")
        require(admin.run(f"SELECT needs_refresh IS NULL FROM hydradb.graphs WHERE name = '{graph}'") == ["t"], "refresh_graph left the flag")
        adjacency_matches_sources(admin, graph)
    # A text key without a length bound registers, with a warning.
    admin.run("CREATE TABLE hydradb_test.m_named (name text PRIMARY KEY, sort_key text NOT NULL)")
    warned = admin.run("""SELECT hydradb.create_graph('named',
        '[{"label": "named", "table": "hydradb_test.m_named", "key": "name", "sort_key": "sort_key"}]')""")
    require(any("has no length bound" in line for line in warned), warned)
    # Each edge endpoint needs an enforced, not deferrable foreign key of its
    # own to the vertex key: without one, a link to a vertex another open
    # transaction is creating waits for it, and two such transactions can
    # deadlock. Losing it later is refused.
    admin.run("CREATE TABLE hydradb_test.fk_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL, UNIQUE (id, sort_key))")
    admin.run("CREATE TABLE hydradb_test.fk_edges (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.fk_vertices, "
              "target bigint NOT NULL REFERENCES hydradb_test.fk_vertices, kind text NOT NULL)")
    fk_graph = single_table_graph("fk_required", "hydradb_test.fk_vertices", "hydradb_test.fk_edges")
    clean = admin.run(fk_graph)
    require(not any("WARNING" in line for line in clean), clean)
    admin.run("ALTER TABLE hydradb_test.fk_edges ALTER CONSTRAINT fk_edges_target_fkey DEFERRABLE",
              error="cannot make foreign key fk_edges_target_fkey on column target of hydradb_test.fk_edges deferrable: "
                    "indexed graph fk_required needs it enforced and not deferrable")
    if int(admin.run("SHOW server_version_num")[0]) >= 180000:
        admin.run("ALTER TABLE hydradb_test.fk_edges ALTER CONSTRAINT fk_edges_target_fkey NOT ENFORCED",
                  error="cannot make foreign key fk_edges_target_fkey on column target of hydradb_test.fk_edges not enforced")
    # A second foreign key on the column keeps the first one droppable.
    fk_flagged = "SELECT needs_refresh FROM hydradb.graphs WHERE name = 'fk_required'"
    admin.run("ALTER TABLE hydradb_test.fk_edges ADD CONSTRAINT fk_edges_target_again FOREIGN KEY (target) REFERENCES hydradb_test.fk_vertices")
    admin.run("ALTER TABLE hydradb_test.fk_edges DROP CONSTRAINT fk_edges_target_fkey")
    require(admin.run(fk_flagged) == [], "dropping one of two foreign keys flagged the graph")
    # Dropping the last one pauses the graph, as pg_dump --clean does before
    # it drops the triggers; a refresh needs it back.
    warned = admin.run("ALTER TABLE hydradb_test.fk_edges DROP CONSTRAINT fk_edges_target_again")
    lost = "lost the foreign key from column target of hydradb_test.fk_edges to key id of hydradb_test.fk_vertices"
    require(any(lost in line for line in warned), warned)
    admin.run(query_sql(graph="fk_required"), error="cannot be queried until hydradb.refresh_graph('fk_required')")
    admin.run("SELECT hydradb.refresh_graph('fk_required')", error="column target of hydradb_test.fk_edges is an edge target with no foreign key")
    admin.run("ALTER TABLE hydradb_test.fk_edges ADD FOREIGN KEY (target) REFERENCES hydradb_test.fk_vertices")
    admin.run("SELECT hydradb.refresh_graph('fk_required')")
    admin.run("SELECT hydradb.drop_graph('fk_required')")
    admin.run("ALTER TABLE hydradb_test.fk_edges DROP CONSTRAINT fk_edges_target_fkey")
    # A multi-column foreign key does not cover the target on its own.
    admin.run("ALTER TABLE hydradb_test.fk_edges ADD FOREIGN KEY (target, kind) REFERENCES hydradb_test.fk_vertices (id, sort_key)")
    missing = admin.run(fk_graph, error="column target of hydradb_test.fk_edges is an edge target with no foreign key to key id of hydradb_test.fk_vertices")
    require(any("ADD FOREIGN KEY (target) REFERENCES hydradb_test.fk_vertices (id)" in line for line in missing), missing)
    admin.run("ALTER TABLE hydradb_test.fk_edges ADD CONSTRAINT fk_edges_target_late FOREIGN KEY (target) "
              "REFERENCES hydradb_test.fk_vertices DEFERRABLE INITIALLY IMMEDIATE")
    deferred = admin.run(fk_graph, error="foreign key fk_edges_target_late on column target of hydradb_test.fk_edges is deferrable")
    require(any("ALTER CONSTRAINT fk_edges_target_late NOT DEFERRABLE" in line for line in deferred), deferred)
    # A NOT VALID one counts: it checks every write from when it was added.
    admin.run("ALTER TABLE hydradb_test.fk_edges DROP CONSTRAINT fk_edges_target_late")
    admin.run("ALTER TABLE hydradb_test.fk_edges ADD FOREIGN KEY (target) REFERENCES hydradb_test.fk_vertices NOT VALID")
    admin.run(fk_graph)
    require(admin.run("SELECT count(*) FROM hydradb.graphs WHERE name = 'fk_required'") == ["1"], "a NOT VALID foreign key was refused")
    print("registration refuses nullable declared columns, edge keys that are not uniquely indexed and edge endpoints "
          "without an enforced, immediate foreign key; an ALTER that would lose any of them is refused, or for a "
          "dropped foreign key pauses the graph")


def test_views_rejected(admin):
    admin.run("CREATE VIEW hydradb_test.vertex_view AS SELECT id, sort_key FROM hydradb_test.vertices")
    admin.run("CREATE VIEW hydradb_test.edge_view AS SELECT id, source, target, kind FROM hydradb_test.edges")
    admin.run("CREATE MATERIALIZED VIEW hydradb_test.vertex_matview AS SELECT id, sort_key FROM hydradb_test.vertices")
    for vertices, edges in (("hydradb_test.vertex_view", "hydradb_test.edges"),
                            ("hydradb_test.vertices", "hydradb_test.edge_view"),
                            ("hydradb_test.vertex_matview", "hydradb_test.edges")):
        admin.run(single_table_graph("from_view", vertices, edges), error="graph sources must be ordinary tables")
    require(admin.run("SELECT count(*) FROM hydradb.graphs WHERE name = 'from_view'") == ["0"], "a rejected view left a graph row")
    # Queries read only the adjacency tables, so a source whose writes they
    # could not all mirror is refused at registration, naming why.
    admin.run("CREATE TABLE hydradb_test.parted (id bigint PRIMARY KEY, sort_key text NOT NULL) PARTITION BY RANGE (id)")
    admin.run(single_table_graph("parted", "hydradb_test.parted", "hydradb_test.edges"),
              error="graph sources must be ordinary tables; hydradb_test.parted is not")
    admin.run("CREATE TABLE hydradb_test.parent_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.child_vertices () INHERITS (hydradb_test.parent_vertices)")
    for table in ("parent_vertices", "child_vertices"):
        admin.run(single_table_graph("inherited", f"hydradb_test.{table}", "hydradb_test.edges"),
                  error=f"hydradb_test.{table} is part of an inheritance tree")
    require(admin.run("SELECT count(*) FROM hydradb.graphs WHERE name IN ('parted', 'inherited')") == ["0"], "a refused graph was registered")
    print("views, materialized views, partitioned tables and inheritance trees are refused as graph sources")


def test_volatile_policy_write(admin):
    admin.run("CREATE TABLE hydradb_test.volatile_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.volatile_edges (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.volatile_vertices, target bigint NOT NULL REFERENCES hydradb_test.volatile_vertices, kind text NOT NULL)")
    admin.run("INSERT INTO hydradb_test.volatile_vertices VALUES (1, 'a'), (2, 'b')")
    admin.run("CREATE FUNCTION hydradb_test.write_from_policy() RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER AS $$ BEGIN INSERT INTO hydradb_test.volatile_edges VALUES (1, 1, 2, 'link') ON CONFLICT DO NOTHING; RETURN true; END $$")
    admin.run("ALTER TABLE hydradb_test.volatile_vertices ENABLE ROW LEVEL SECURITY")
    admin.run("CREATE POLICY writing ON hydradb_test.volatile_vertices USING (hydradb_test.write_from_policy())")
    admin.run("GRANT SELECT ON hydradb_test.volatile_vertices, hydradb_test.volatile_edges TO hydradb_test_reader")
    admin.run(single_table_graph("volatile_source", "hydradb_test.volatile_vertices", "hydradb_test.volatile_edges"))
    with Session() as reader:
        reader.run("SET ROLE hydradb_test_reader")
        reader.run(query_sql(graph="volatile_source"), error="row-level security on volatile_vertices applies to it")
    require(admin.run("SELECT count(*) FROM hydradb_test.volatile_edges") == ["0"], "a refused query ran the source's policy")
    # A policy that writes and hides every row is never run either.
    admin.run("CREATE TABLE hydradb_test.policy_log (at timestamptz)")
    admin.run("CREATE FUNCTION hydradb_test.write_and_hide() RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER AS $$ BEGIN INSERT INTO hydradb_test.policy_log VALUES (now()); RETURN false; END $$")
    admin.run("CREATE TABLE hydradb_test.plain_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("INSERT INTO hydradb_test.plain_vertices VALUES (1, 'a'), (2, 'b')")
    admin.run("CREATE TABLE hydradb_test.hidden_edges (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.plain_vertices, "
              "target bigint NOT NULL REFERENCES hydradb_test.plain_vertices, kind text NOT NULL)")
    admin.run("INSERT INTO hydradb_test.hidden_edges VALUES (1, 1, 2, 'link')")
    admin.run("ALTER TABLE hydradb_test.hidden_edges ENABLE ROW LEVEL SECURITY")
    admin.run("CREATE POLICY hiding ON hydradb_test.hidden_edges USING (hydradb_test.write_and_hide())")
    admin.run("GRANT SELECT ON hydradb_test.plain_vertices, hydradb_test.hidden_edges TO hydradb_test_reader")
    admin.run(single_table_graph("hidden_edges", "hydradb_test.plain_vertices", "hydradb_test.hidden_edges"))
    with Session() as reader:
        reader.run("SET ROLE hydradb_test_reader")
        reader.run(query_sql(graph="hidden_edges"), error="row-level security on hidden_edges applies to it")
    require(admin.run("SELECT count(*) FROM hydradb_test.policy_log") == ["0"], "a refused query ran the source's policy")
    print("a query row-level security would filter is refused before any policy runs, so a volatile policy never writes")


def test_result_names_charged(admin):
    """Names are charged no more than they serialize to: 1,024 paths whose
    JSON fits a result budget are returned, and a budget they exceed fails."""
    create_tables(admin, "hydradb_test.parallel_vertices", "hydradb_test.parallel_edges")
    admin.run("INSERT INTO hydradb_test.parallel_vertices VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    admin.run("INSERT INTO hydradb_test.parallel_edges SELECT n, 1, 2, 'link' FROM generate_series(1, 32) n")
    admin.run("INSERT INTO hydradb_test.parallel_edges SELECT 100 + n, 2, 3, 'link' FROM generate_series(1, 32) n")
    admin.run(single_table_graph("parallel", "hydradb_test.parallel_vertices", "hydradb_test.parallel_edges"))
    options = {"edge_kinds": ["link"], "max_depth": 2, "paths_per_pair": 0}
    paths = by_key(admin, "paths_between", {"vertex": 1}, {"vertex": 3}, graph="parallel", options={**options, "max_result_bytes": 200_000})
    require(len(paths["items"]) == 1024, len(paths["items"]))
    by_key(admin, "paths_between", {"vertex": 1}, {"vertex": 3}, graph="parallel", options={**options, "max_result_bytes": 100_000},
           error="max_result_bytes")
    print("named results are charged as they serialize: 1,024 paths fit their budget, and a smaller one fails")


def test_sources_and_key_types(admin):
    """Keys compare as text, so only types whose text identifies a value may be
    keys; a source is read by the relation registered, whatever its name now."""
    admin.run("CREATE TABLE hydradb_test.num_vertices (id numeric PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.num_edges (id bigint PRIMARY KEY, source numeric NOT NULL REFERENCES hydradb_test.num_vertices, target numeric NOT NULL REFERENCES hydradb_test.num_vertices, kind text NOT NULL)")
    admin.run(single_table_graph("numeric_keys", "hydradb_test.num_vertices", "hydradb_test.num_edges"),
              error="column id of hydradb_test.num_vertices is a graph key or endpoint of type numeric")
    # A case-insensitive collation calls differently spelled keys equal, so
    # it is refused for keys at registration, and at every load once a
    # registered key column takes it.
    # Only ICU provides nondeterministic collations, and it is an optional
    # build feature, so without it these two checks are skipped.
    icu = admin.run("SELECT EXISTS (SELECT 1 FROM pg_collation WHERE collprovider = 'i')") == ["t"]
    if icu:
        admin.run("CREATE COLLATION hydradb_test.case_insensitive (provider = icu, locale = 'und-u-ks-level2', deterministic = false)")
        admin.run("CREATE TABLE hydradb_test.ci_vertices (id text COLLATE hydradb_test.case_insensitive PRIMARY KEY, sort_key text NOT NULL)")
        # No foreign keys: PostgreSQL 18 refuses one between columns of
        # different collations when either is nondeterministic, and the
        # registration below is refused for the key's collation before any
        # foreign key is checked.
        admin.run("CREATE TABLE hydradb_test.ci_edges (id bigint PRIMARY KEY, source text NOT NULL, target text NOT NULL, kind text NOT NULL)")
        vertex_tables = [{"label": "vertex", "table": "hydradb_test.ci_vertices", "key": "id", "sort_key": "sort_key"}]
        admin.run(f"SELECT hydradb.create_graph('case_insensitive', {literal(json.dumps(vertex_tables))})",
                  error="column id of hydradb_test.ci_vertices is a graph key or endpoint with nondeterministic collation case_insensitive")
        admin.run("CREATE TABLE hydradb_test.cs_vertices (id text PRIMARY KEY, sort_key text NOT NULL)")
        admin.run("INSERT INTO hydradb_test.cs_vertices VALUES ('Alice', 'a')")
        vertex_tables = [{"label": "vertex", "table": "hydradb_test.cs_vertices", "key": "id", "sort_key": "sort_key"}]
        admin.run(f"SELECT hydradb.create_graph('recollated', {literal(json.dumps(vertex_tables))})")
        recollate = "ALTER TABLE hydradb_test.cs_vertices ALTER COLUMN id TYPE text COLLATE hydradb_test.case_insensitive"
        admin.run(recollate, error="cannot change column id of hydradb_test.cs_vertices: it is the key of indexed graph recollated")
        # With the event triggers off, as a superuser can turn them, the change
        # goes through; queries refuse the collation, and so does a refresh,
        # whose registration no longer describes the column.
        admin.run("ALTER EVENT TRIGGER pg_hydradb_check_altered_sources DISABLE")
        admin.run(recollate)
        admin.run("ALTER EVENT TRIGGER pg_hydradb_check_altered_sources ENABLE")
        by_key(admin, "reachable_vertices", {"vertex": "Alice"}, graph="recollated",
               error="with a nondeterministic collation")
        admin.run("SELECT hydradb.refresh_graph('recollated')",
                  error='the graph\'s key, was text COLLATE pg_catalog."default" NOT NULL at registration and is now '
                        'text COLLATE hydradb_test.case_insensitive NOT NULL')
    else:
        print("case-insensitive key collations not checked: this PostgreSQL has no ICU")
    # A temporary table cannot back a registration that outlives it.
    with Session() as temporary:
        temporary.run("CREATE TEMP TABLE temp_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL)")
        vertex_tables = [{"label": "vertex", "table": "pg_temp.temp_vertices", "key": "id", "sort_key": "sort_key"}]
        temporary.run(f"SELECT hydradb.create_graph('temporary', {literal(json.dumps(vertex_tables))})",
                      error="graph sources must be permanent tables")
    admin.run("CREATE TABLE hydradb_test.retyped_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.retyped_edges (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.retyped_vertices, target bigint NOT NULL REFERENCES hydradb_test.retyped_vertices, kind text NOT NULL)")
    admin.run("INSERT INTO hydradb_test.retyped_vertices VALUES (1, 'a'), (2, 'b')")
    admin.run("INSERT INTO hydradb_test.retyped_edges VALUES (1, 1, 2, 'link')")
    admin.run(single_table_graph("retyped", "hydradb_test.retyped_vertices", "hydradb_test.retyped_edges"))
    # A table renamed, and another moved into its old name, while a repeatable
    # read snapshot is open: the query reads the registered table.
    create_tables(admin, "hydradb_test.swap_vertices", "hydradb_test.swap_edges")
    admin.run("INSERT INTO hydradb_test.swap_vertices VALUES (1, 'a'), (2, 'b')")
    admin.run("INSERT INTO hydradb_test.swap_edges VALUES (1, 1, 2, 'link')")
    admin.run(single_table_graph("swapped", "hydradb_test.swap_vertices", "hydradb_test.swap_edges"))
    admin.run("CREATE TABLE hydradb_test.impostor (LIKE hydradb_test.swap_vertices INCLUDING ALL)")
    admin.run("INSERT INTO hydradb_test.impostor VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    with Session() as snapshot:
        snapshot.run("BEGIN ISOLATION LEVEL REPEATABLE READ")
        snapshot.run("SELECT 1")
        admin.run("ALTER TABLE hydradb_test.swap_vertices RENAME TO swap_vertices_moved")
        admin.run("ALTER TABLE hydradb_test.impostor RENAME TO swap_vertices")
        swapped = query(snapshot, graph="swapped", options={"max_depth": 10})
        require(ids(swapped) == [1, 2], swapped)
        snapshot.run("COMMIT")
    # Without a kind column each edge's kind is its table's label, charged as
    # a kind column's value would be.
    admin.run("CREATE TABLE hydradb_test.kindless (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.retyped_vertices, "
              "target bigint NOT NULL REFERENCES hydradb_test.retyped_vertices)")
    admin.run("INSERT INTO hydradb_test.kindless SELECT n, 1, 2 FROM generate_series(1, 10) n")
    for name, label in (("short_label", "e"), ("long_label", "e" * 1001)):
        vertex_tables = [{"label": "vertex", "table": "hydradb_test.retyped_vertices", "key": "id", "sort_key": "sort_key"}]
        edge_tables = [{"label": label, "table": "hydradb_test.kindless", "key": "id", "source": "source",
                        "source_label": "vertex", "target": "target", "target_label": "vertex"}]
        admin.run(f"SELECT hydradb.create_graph({literal(name)}, {literal(json.dumps(vertex_tables))}, {literal(json.dumps(edge_tables))})")
    # The 10 edges the walk loads are charged their label.
    walked = lambda graph: query(admin, graph=graph, options={"max_depth": 1})["storage"]["estimated_bytes"]
    require(walked("long_label") - walked("short_label") == 10 * 1000, (walked("long_label"), walked("short_label")))
    # Invalid path options are refused before the graph is loaded: a budget
    # the graph exceeds is never reached.
    by_key(admin, "paths_from", {"vertex": 1}, options={"direction": "sideways", "max_snapshot_bytes": 1}, graph="retyped",
           error="direction must be")
    print("keys and endpoints must have a type and collation whose text identifies a value, rechecked at every load; "
          "temporary tables are refused; "
          "a renamed source is still read; default kinds are charged; invalid path options load nothing")


def test_concurrency(writer):
    with Session() as reader, Session() as repeatable:
        initial = query(reader, graph="mvcc")
        require(ids(initial) == [1, 2], initial)
        writer.run("BEGIN; INSERT INTO hydradb_test.mvcc_edges VALUES (2, 2, 3, 'link')")
        require(ids(query(reader, graph="mvcc")) == [1, 2], "an uncommitted edge was visible")
        repeatable.run("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        require(ids(query(repeatable, graph="mvcc")) == [1, 2], "repeatable read saw an uncommitted edge")
        writer.run("COMMIT")
        require(ids(query(reader, graph="mvcc")) == [1, 2, 3], "a committed edge was not visible")
        require(ids(query(repeatable, graph="mvcc")) == [1, 2], "repeatable read saw a later commit")
        repeatable.run("COMMIT")
        require(ids(query(repeatable, graph="mvcc")) == [1, 2, 3], "repeatable-read commit did not advance snapshot")
        reader.run("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY")
        before = query(reader, graph="mvcc")
        writer.run("INSERT INTO hydradb_test.mvcc_edges VALUES (3, 3, 4, 'link')")
        after = query(reader, graph="mvcc")
        require(ids(before) == [1, 2, 3] and ids(after) == [1, 2, 3, 4], after)
        reader.run("COMMIT")
        (Path(os.environ["PGHYDRADB_TEST_ROOT"]) / "recovery.json").write_text(json.dumps({"items": after["items"]}))
    print("concurrent uncommitted/committed visibility and repeatable-read/read-committed snapshots")


def test_recovery():
    expected = json.loads((Path(os.environ["PGHYDRADB_TEST_ROOT"]) / "recovery.json").read_text())
    with Session() as session:
        recovered = query(session, graph="mvcc")
        require(recovered["items"] == expected["items"], recovered)
    print("immediate-stop WAL recovery preserves the committed adjacency tables")


DOC = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"


def by_key(session, operation, *seeds, options=None, error=None, graph="org"):
    """Calls `operation` with seeds by label and key; returns the result with
    its items in `normalized` order, or the lines of the expected error."""
    arguments = [literal(graph)] + [literal(json.dumps(seed)) + "::jsonb" for seed in seeds]
    arguments.append(literal(json.dumps(options or {})) + "::jsonb")
    sql = f"SELECT hydradb.{operation}({', '.join(arguments)})"
    if error is not None:
        return session.run(sql, error=error)
    result = session.json(sql)
    require_storage(result)
    return normalized(result, operation)


def named(label, key, depth=None):
    item = {"label": label, "key": key}
    return item if depth is None else {**item, "depth": depth}


def test_multi_table(admin):
    admin.run("CREATE TABLE hydradb_test.people (id integer PRIMARY KEY, name text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.companies (slug text PRIMARY KEY)")
    admin.run("CREATE TABLE hydradb_test.documents (id uuid PRIMARY KEY, title text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.employment (id bigint PRIMARY KEY, person_id bigint NOT NULL REFERENCES hydradb_test.people, "
              "company_slug text NOT NULL REFERENCES hydradb_test.companies, months integer)")
    admin.run("CREATE TABLE hydradb_test.knows (id bigint PRIMARY KEY, a integer NOT NULL REFERENCES hydradb_test.people, "
              "b integer NOT NULL REFERENCES hydradb_test.people)")
    admin.run("CREATE TABLE hydradb_test.authorship (id text PRIMARY KEY, doc_id uuid NOT NULL REFERENCES hydradb_test.documents, "
              "person_id integer NOT NULL REFERENCES hydradb_test.people)")
    admin.run("INSERT INTO hydradb_test.people VALUES (1, 'ann'), (2, 'bob'), (10, 'cat')")
    admin.run("INSERT INTO hydradb_test.companies VALUES ('acme'), ('zeta')")
    admin.run(f"INSERT INTO hydradb_test.documents VALUES ('{DOC}', 'design')")
    admin.run("INSERT INTO hydradb_test.employment VALUES (1, 1, 'acme', 7), (2, 2, 'acme', 3), (3, 10, 'zeta', 5)")
    admin.run("INSERT INTO hydradb_test.knows VALUES (1, 1, 2), (2, 2, 10)")
    admin.run(f"INSERT INTO hydradb_test.authorship VALUES ('w1', '{DOC}', 1)")
    vertex_tables = [
        {"label": "person", "table": "hydradb_test.people", "key": "id", "sort_key": "name"},
        {"label": "company", "table": "hydradb_test.companies", "key": "slug"},
        {"label": "document", "table": "hydradb_test.documents", "key": "id", "sort_key": "title"},
    ]
    edge_tables = [
        {"label": "works_at", "table": "hydradb_test.employment", "key": "id",
         "source": "person_id", "source_label": "person", "target": "company_slug", "target_label": "company"},
        {"label": "knows", "table": "hydradb_test.knows", "key": "id",
         "source": "a", "source_label": "person", "target": "b", "target_label": "person"},
        {"label": "written_by", "table": "hydradb_test.authorship", "key": "id",
         "source": "doc_id", "source_label": "document", "target": "person_id", "target_label": "person"},
    ]

    def create(name, vertices, edges, error=None):
        sql = f"SELECT hydradb.create_graph({literal(name)}, {literal(json.dumps(vertices))}, {literal(json.dumps(edges))})"
        return admin.run(sql, error=error)

    works_at = edge_tables[0]
    create("bad", [{**vertex_tables[0], "table": "people"}], [], error="schema-qualified")
    create("bad", [{**vertex_tables[0], "colour": "red"}], [], error="unknown table declaration field")
    create("bad", [vertex_tables[0]], [], error=None)
    admin.run("SELECT hydradb.drop_graph('bad')")
    create("bad", vertex_tables[:1], [works_at], error="must name a vertex table")
    create("bad", vertex_tables, [{**works_at, "source": "company_slug"}], error="has type")
    create("bad", vertex_tables, [{**works_at, "key": "missing"}], error="does not exist")
    create("bad", vertex_tables + vertex_tables[:1], [], error="unique")
    require(admin.run("SELECT count(*) FROM hydradb.graphs WHERE name = 'bad'") == ["0"], "failed registration left a graph row")
    create("org", vertex_tables, edge_tables)
    require(admin.run("SELECT to_regprocedure('hydradb.register_graph(text,regclass,regclass)') IS NULL") == ["t"], "register_graph still exists")
    triggers = admin.run("SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'pg_hydradb_' || md5('org') || '\\_%' AND tgenabled = 'A'")
    require(triggers == ["24"], f"expected four maintenance triggers on each of six tables: {triggers}")
    adjacency_matches_sources(admin, "org")

    first = by_key(admin, "reachable_vertices", {"person": [1]}, options={"max_depth": 2})
    require(first["items"] == [named("person", 1, 0), named("company", "acme", 1), named("person", 2, 1),
                               named("person", 10, 2)], first)
    typed = by_key(admin, "reachable_vertices", {"person": 1}, options={"edge_kind": "works_at", "max_depth": 5})
    require(typed["items"] == [named("person", 1, 0), named("company", "acme", 1)], typed)
    incoming = by_key(admin, "reachable_vertices", {"company": ["acme"]}, options={"direction": "incoming", "max_depth": 1})
    require(incoming["items"] == [named("company", "acme", 0), named("person", 1, 1), named("person", 2, 1)], incoming)
    mixed = by_key(admin, "reachable_vertices", {"company": "zeta", "document": DOC}, options={"max_depth": 0})
    require(mixed["items"] == [named("company", "zeta", 0), named("document", DOC, 0)], mixed)
    edges = by_key(admin, "reachable_edges", {"person": [1]}, options={"direction": "both", "max_depth": 1})
    require(edges["items"] == [named("knows", 1, 1), named("works_at", 1, 1), named("written_by", "w1", 1)], edges)
    paths = by_key(admin, "paths_between", {"document": DOC}, {"company": ["zeta"]}, options={"max_depth": 10})
    require(paths["items"] == [{
        "vertices": [named("document", DOC), named("person", 1), named("person", 2), named("person", 10), named("company", "zeta")],
        "edges": [named("written_by", "w1"), named("knows", 1), named("knows", 2), named("works_at", 3)],
    }], paths)
    direct = by_key(admin, "paths_between", {"person": 2}, {"company": "acme"}, options={"edge_kinds": ["works_at"]})
    require(direct["items"] == [{"vertices": [named("person", 2), named("company", "acme")],
                                 "edges": [named("works_at", 2)]}], direct)
    longer = by_key(admin, "paths_between", {"person": 1}, {"person": 10}, options={"edge_kinds": ["knows"]})
    require([len(p["edges"]) for p in longer["items"]] == [2], longer)
    # The graph is unweighted: weight and cost options do not exist.
    for option in ({"weightProp": "months"}, {"costProp": "months"}, {"maxCost": 5}):
        by_key(admin, "paths_between", {"person": 2}, {"company": "acme"}, options={"edge_kinds": ["works_at"], **option},
               error="unknown field")
    ms = by_key(admin, "paths_from", {"person": [10]}, options={"edge_kinds": ["knows", "works_at"], "max_depth": 1})
    require(all(p["vertices"][0] == named("person", 10) for p in ms["items"]) and ms["items"], ms)
    for options in ({"edge_kinds": ["knows", "works_at", "written_by"], "direction": "both", "max_depth": 3, "paths_per_pair": 0},
                    {"edge_kinds": ["knows"], "direction": "incoming", "paths_per_pair": 3}):
        by_key(admin, "paths_from", {"person": 10}, options=options)
        by_key(admin, "paths_between", {"person": [1, 2, 10]}, {"company": ["acme", "zeta"]}, options=options)

    by_key(admin, "reachable_vertices", {"person": [99]}, error="missing vertex person 99")
    by_key(admin, "paths_between", {"person": 1}, {"company": "nope"}, options={"edge_kinds": ["works_at"]},
           error='missing vertex company "nope"')
    by_key(admin, "reachable_vertices", {"robot": [1]}, error="unknown vertex label")
    # The JSON type must match the key column; nothing is converted or rewritten.
    for seed in ("1", 1.5, None):
        by_key(admin, "reachable_vertices", {"person": [seed]}, error="pass a JSON integer")
    by_key(admin, "reachable_vertices", {"company": [5]}, error="pass a JSON string")
    by_key(admin, "reachable_vertices", {"document": DOC.upper()}, error="missing vertex document")
    by_key(admin, "reachable_vertices", [1], error="JSON object")
    require(admin.run("SELECT to_regprocedure('hydradb.reachable_vertices(text,bigint[],jsonb)') IS NULL") == ["t"], "number-only reachable still exists")
    for old in ("reachable", "nearby_edges", "shortest_paths", "sp_paths", "ss_paths", "ms_paths", "ms_paths_between"):
        require(admin.run(f"SELECT to_regproc('hydradb.{old}') IS NULL") == ["t"], f"{old} still exists")

    with Session() as reader:
        reader.run("SET ROLE hydradb_test_reader")
        admin.run("GRANT SELECT ON hydradb_test.people, hydradb_test.companies, hydradb_test.documents, hydradb_test.employment, hydradb_test.knows TO hydradb_test_reader")
        by_key(reader, "reachable_vertices", {"person": [1]}, error="privileges")
        admin.run("GRANT SELECT ON hydradb_test.authorship TO hydradb_test_reader")
        require(by_key(reader, "reachable_vertices", {"person": [1]})["items"][0] == named("person", 1, 0), "grant on the last table did not restore access")
        admin.run("INSERT INTO hydradb_test.authorship VALUES ('w2', '" + DOC + "', 10)")
        changed = normalized(by_key(reader, "reachable_vertices", {"document": DOC}, options={"max_depth": 1}))
        require(changed["items"] == [named("document", DOC, 0), named("person", 1, 1), named("person", 10, 1)], changed)

    # A dangling endpoint is an error once a query reaches it. The foreign key
    # stops one, except in a replica-role write, which skips foreign key
    # checks but fires the graph's triggers.
    admin.run("INSERT INTO hydradb_test.employment VALUES (4, 1, 'nope', 1)", error="violates foreign key constraint")
    admin.run("SET session_replication_role = replica; INSERT INTO hydradb_test.employment VALUES (4, 1, 'nope', 1); "
              "RESET session_replication_role")
    by_key(admin, "reachable_vertices", {"person": [1]}, error='refers to missing vertex company "nope"')
    admin.run("DELETE FROM hydradb_test.employment WHERE id = 4")
    adjacency_matches_sources(admin, "org")
    admin.run("CREATE TABLE hydradb_test.twice AS SELECT slug FROM hydradb_test.companies UNION ALL SELECT 'acme'")
    admin.run("ALTER TABLE hydradb_test.twice ALTER COLUMN slug SET NOT NULL")
    dupes = """SELECT hydradb.create_graph('dupes', '[{"label": "company", "table": "hydradb_test.twice", "key": "slug"}]')"""
    admin.run(dupes, error="vertex key slug of hydradb_test.twice has no unique index of its own")
    print("multi-table registration, keyed seeds and results, paths over a loaded region, privileges and dangling endpoints passed")


def test_maintenance(admin):
    admin.run("CREATE TABLE hydradb_test.inc_people (id integer PRIMARY KEY, name text NOT NULL, note text)")
    admin.run("CREATE TABLE hydradb_test.inc_companies (slug text PRIMARY KEY)")
    admin.run("CREATE TABLE hydradb_test.inc_jobs (id bigint PRIMARY KEY, "
              "person_id integer NOT NULL REFERENCES hydradb_test.inc_people ON UPDATE CASCADE, "
              "company_slug text NOT NULL REFERENCES hydradb_test.inc_companies ON UPDATE CASCADE)")
    admin.run("INSERT INTO hydradb_test.inc_people VALUES (1, 'ann', NULL), (2, 'bob', NULL), (3, 'cat', NULL)")
    admin.run("INSERT INTO hydradb_test.inc_companies VALUES ('acme'), ('zeta')")
    admin.run("INSERT INTO hydradb_test.inc_jobs VALUES (1, 1, 'acme'), (2, 2, 'acme'), (3, 3, 'zeta')")
    admin.run("""SELECT hydradb.create_graph('inc',
        '[{"label": "person", "table": "hydradb_test.inc_people", "key": "id", "sort_key": "name"},
          {"label": "company", "table": "hydradb_test.inc_companies", "key": "slug"}]',
        '[{"label": "works_at", "table": "hydradb_test.inc_jobs", "key": "id",
           "source": "person_id", "source_label": "person", "target": "company_slug", "target_label": "company"}]')""")
    seeds = [{"person": 1}, {"company": "acme"}]
    rows = lambda: admin.run("SELECT string_agg(xmin::text || ':' || ctid::text, ',' ORDER BY vid) FROM hydradb.graph_vertices v "
                             "WHERE graph = (SELECT registration FROM hydradb.graphs WHERE name = 'inc')")

    def check():
        adjacency_matches_sources(admin, "inc")
        for seed in seeds:
            for operation in ("reachable_vertices", "reachable_edges"):
                by_key(reader, operation, seed, options={"direction": "both", "max_depth": 4}, graph="inc")

    def step(sql, touches_vertices=True):
        before = rows()
        admin.run(sql)
        check()
        require((rows() != before) == touches_vertices, f"{sql}: vertex rows {'unchanged' if touches_vertices else 'rewritten'}")

    with Session() as reader:
        check()
        # Columns the graph does not read, or declared ones set to the same
        # value, leave the adjacency tables alone.
        step("UPDATE hydradb_test.inc_people SET note = 'x'", touches_vertices=False)
        step("UPDATE hydradb_test.inc_people SET name = name", touches_vertices=False)
        # A BEFORE trigger that changes a declared column is maintained although
        # the UPDATE names only an undeclared one.
        admin.run("CREATE FUNCTION hydradb_test.rename() RETURNS trigger LANGUAGE plpgsql AS "
                  "$$ BEGIN IF NEW.note = 'rename' THEN NEW.name := 'zed'; END IF; RETURN NEW; END $$")
        admin.run("CREATE TRIGGER rename BEFORE UPDATE ON hydradb_test.inc_people "
                  "FOR EACH ROW EXECUTE FUNCTION hydradb_test.rename()")
        step("UPDATE hydradb_test.inc_people SET note = 'rename' WHERE id = 1")
        admin.run("DROP TRIGGER rename ON hydradb_test.inc_people")
        step("BEGIN; INSERT INTO hydradb_test.inc_people VALUES (4, 'dan', NULL); "
             "INSERT INTO hydradb_test.inc_jobs VALUES (4, 4, 'zeta'); COMMIT")
        step("UPDATE hydradb_test.inc_people SET name = 'aaa' WHERE id = 3")
        step("UPDATE hydradb_test.inc_jobs SET company_slug = 'acme' WHERE id = 4", touches_vertices=False)
        # A key changed, and the change cascaded to the edges that name it.
        step("UPDATE hydradb_test.inc_people SET id = 9 WHERE id = 3")
        step("UPDATE hydradb_test.inc_companies SET slug = 'zed' WHERE slug = 'zeta'")
        step("UPDATE hydradb_test.inc_companies SET slug = 'zeta' WHERE slug = 'zed'")
        step("BEGIN; DELETE FROM hydradb_test.inc_jobs WHERE person_id = 4; "
             "DELETE FROM hydradb_test.inc_people WHERE id = 4; COMMIT")
        step("BEGIN; INSERT INTO hydradb_test.inc_companies VALUES ('nova'); "
             "UPDATE hydradb_test.inc_jobs SET company_slug = 'nova' WHERE id = 3; "
             "INSERT INTO hydradb_test.inc_people VALUES (6, 'eve', NULL); COMMIT")
        # A rolled-back write leaves nothing.
        before = rows()
        admin.run("BEGIN; INSERT INTO hydradb_test.inc_people VALUES (7, 'fay', NULL)")
        admin.run("ROLLBACK")
        require(rows() == before, "a rolled-back write changed the adjacency tables")
        # Null declared fields fail the write rather than the graph.
        admin.run("INSERT INTO hydradb_test.inc_people VALUES (8, NULL, NULL)", error="null")
        # A one-edge write touches one adjacency row and no others.
        admin.run("INSERT INTO hydradb_test.inc_people SELECT n, 'p' || n, NULL FROM generate_series(100, 1200) n")
        admin.run("INSERT INTO hydradb_test.inc_jobs SELECT n, n, 'acme' FROM generate_series(100, 1200) n")
        before = admin.run("SELECT string_agg(xmin::text, ',' ORDER BY label, key) FROM hydradb.graph_adjacency WHERE graph = (SELECT registration FROM hydradb.graphs WHERE name = 'inc')")[0].split(",")
        admin.run("INSERT INTO hydradb_test.inc_jobs VALUES (5000, 1, 'zeta')")
        after = admin.run("SELECT string_agg(xmin::text, ',' ORDER BY label, key) FROM hydradb.graph_adjacency WHERE graph = (SELECT registration FROM hydradb.graphs WHERE name = 'inc')")[0].split(",")
        require(len(after) == len(before) + 1 and len(set(after) - set(before)) == 1, "a one-edge insert rewrote other adjacency rows")
        check()
        admin.run("TRUNCATE hydradb_test.inc_jobs")
        check()
        admin.run("INSERT INTO hydradb_test.inc_jobs VALUES (1, 1, 'acme'), (2, 2, 'zeta')")
        admin.run("SELECT hydradb.refresh_graph('inc')")
        check()
        # The adjacency tables are private to the extension.
        with Session() as stranger:
            stranger.run("SET ROLE hydradb_test_reader")
            stranger.run("SELECT count(*) FROM hydradb.graph_adjacency", error="permission denied")
    print("maintenance: undeclared columns ignored; inserts, updates, key swaps, deletes, truncate and refresh mirror the tables; one-edge writes touch one row")


def test_user_after_triggers(admin):
    """A user's AFTER ROW trigger that writes the same table runs, with its
    nested statement's own maintenance, before pg_hydradb's statement trigger
    for the outer statement. That one must mirror the table as it is then, not
    the outer statement's transition rows, which no longer match it."""
    admin.run("CREATE TABLE hydradb_test.at_vertices (id integer PRIMARY KEY, sort_key text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.at_edges (id integer PRIMARY KEY, "
              "source integer NOT NULL REFERENCES hydradb_test.at_vertices, "
              "target integer NOT NULL REFERENCES hydradb_test.at_vertices, kind text NOT NULL)")
    admin.run("INSERT INTO hydradb_test.at_vertices VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    admin.run("INSERT INTO hydradb_test.at_edges VALUES (1, 1, 2, 'x')")
    admin.run(single_table_graph("after_triggers", "hydradb_test.at_vertices", "hydradb_test.at_edges"))
    # Each branch acts once, at depth 1, so its own writes do not trigger it again.
    admin.run("""CREATE FUNCTION hydradb_test.at_edge_rules() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF pg_trigger_depth() > 1 THEN RETURN NULL; END IF;
            IF TG_OP = 'INSERT' AND NEW.source = NEW.target THEN
                DELETE FROM hydradb_test.at_edges WHERE id = NEW.id;
            ELSIF TG_OP = 'INSERT' AND NEW.kind = 'fix' THEN
                UPDATE hydradb_test.at_edges SET kind = 'fixed', target = 3 WHERE id = NEW.id;
            ELSIF TG_OP = 'INSERT' AND NEW.kind = 'pair' THEN
                INSERT INTO hydradb_test.at_edges VALUES (NEW.id + 1000, NEW.target, NEW.source, 'reverse');
            ELSIF TG_OP = 'DELETE' AND OLD.kind = 'keep' THEN
                INSERT INTO hydradb_test.at_edges VALUES (OLD.id, OLD.source, 3, 'kept');
            ELSIF TG_OP = 'UPDATE' AND NEW.kind = 'undo' THEN
                UPDATE hydradb_test.at_edges SET kind = 'redone', target = OLD.target WHERE id = NEW.id;
            END IF;
            RETURN NULL;
        END $$""")
    admin.run("CREATE TRIGGER a_rules AFTER INSERT OR UPDATE OR DELETE ON hydradb_test.at_edges "
              "FOR EACH ROW EXECUTE FUNCTION hydradb_test.at_edge_rules()")
    admin.run("""CREATE FUNCTION hydradb_test.at_vertex_rules() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF pg_trigger_depth() > 1 THEN RETURN NULL; END IF;
            IF TG_OP = 'INSERT' AND NEW.sort_key = 'drop' THEN
                DELETE FROM hydradb_test.at_vertices WHERE id = NEW.id;
            ELSIF TG_OP = 'UPDATE' AND NEW.sort_key = 'bump' THEN
                UPDATE hydradb_test.at_vertices SET sort_key = 'bumped' WHERE id = NEW.id;
            END IF;
            RETURN NULL;
        END $$""")
    admin.run("CREATE TRIGGER a_rules AFTER INSERT OR UPDATE ON hydradb_test.at_vertices "
              "FOR EACH ROW EXECUTE FUNCTION hydradb_test.at_vertex_rules()")
    edges = lambda: admin.run("SELECT string_agg(format('%s:%s>%s:%s', id, source, target, kind), ' ' ORDER BY id) "
                              "FROM hydradb_test.at_edges")
    for sql, expected in (
        # delete-on-insert: the self link is gone from the table.
        ("INSERT INTO hydradb_test.at_edges VALUES (10, 1, 1, 'x'), (11, 1, 3, 'x')", "1:1>2:x 11:1>3:x"),
        # update-on-insert: the row stored differs from the one inserted.
        ("INSERT INTO hydradb_test.at_edges VALUES (12, 1, 2, 'fix')", "1:1>2:x 11:1>3:x 12:1>3:fixed"),
        # insert-on-insert: a new row of its own.
        ("INSERT INTO hydradb_test.at_edges VALUES (13, 1, 2, 'pair')", "1:1>2:x 11:1>3:x 12:1>3:fixed 13:1>2:pair 1013:2>1:reverse"),
        # update-on-update: the nested update reverts part of the outer one.
        ("UPDATE hydradb_test.at_edges SET kind = 'undo', target = 3 WHERE id = 13",
         "1:1>2:x 11:1>3:x 12:1>3:fixed 13:1>2:redone 1013:2>1:reverse"),
        # insert-on-delete, reusing the deleted key.
        ("UPDATE hydradb_test.at_edges SET kind = 'keep' WHERE id = 11", None),
        ("DELETE FROM hydradb_test.at_edges WHERE id = 11", "1:1>2:x 11:1>3:kept 12:1>3:fixed 13:1>2:redone 1013:2>1:reverse"),
        # The same on a vertex table: a vertex deleted as it is inserted, and a
        # sort key changed again after an update.
        ("INSERT INTO hydradb_test.at_vertices VALUES (9, 'drop'), (8, 'h')", None),
        ("UPDATE hydradb_test.at_vertices SET sort_key = 'bump' WHERE id = 1", None),
    ):
        admin.run(sql)
        if expected is not None:
            require(edges() == [expected], f"{sql}: the user's trigger left {edges()}, not {expected}")
        adjacency_matches_sources(admin, "after_triggers")
    require(admin.run("SELECT string_agg(id || ':' || sort_key, ' ' ORDER BY id) FROM hydradb_test.at_vertices")
            == ["1:bumped 2:b 3:c 8:h"], "the vertex rules did not run")
    with Session() as reader:
        by_key(reader, "reachable_vertices", {"vertex": 1}, options={"direction": "both", "max_depth": 3}, graph="after_triggers")
        by_key(reader, "reachable_vertices", {"vertex": 9}, graph="after_triggers", error="missing vertex vertex 9")
    # Declared columns named like the aliases the UPDATE comparison uses.
    admin.run("CREATE TABLE hydradb_test.at_named_vertices (k bigint PRIMARY KEY, c1 text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.at_named_edges (pg_hydradb_key bigint PRIMARY KEY, k bigint NOT NULL REFERENCES hydradb_test.at_named_vertices, "
              "c1 bigint NOT NULL REFERENCES hydradb_test.at_named_vertices, c2 text NOT NULL)")
    admin.run("INSERT INTO hydradb_test.at_named_vertices VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    admin.run("INSERT INTO hydradb_test.at_named_edges VALUES (1, 1, 2, 'x')")
    admin.run("""SELECT hydradb.create_graph('at_named',
        '[{"label": "vertex", "table": "hydradb_test.at_named_vertices", "key": "k", "sort_key": "c1"}]',
        '[{"label": "edge", "table": "hydradb_test.at_named_edges", "key": "pg_hydradb_key", "source": "k",
           "source_label": "vertex", "target": "c1", "target_label": "vertex", "kind": "c2"}]')""")
    admin.run("CREATE FUNCTION hydradb_test.at_nothing() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$")
    for table in ("at_named_vertices", "at_named_edges"):
        admin.run(f"CREATE TRIGGER a_nothing AFTER UPDATE ON hydradb_test.{table} FOR EACH ROW EXECUTE FUNCTION hydradb_test.at_nothing()")
    admin.run("UPDATE hydradb_test.at_named_edges SET c1 = 3, c2 = 'y'")
    admin.run("UPDATE hydradb_test.at_named_vertices SET c1 = 'z' WHERE k = 1")
    adjacency_matches_sources(admin, "at_named")
    print("a user's AFTER ROW trigger that deletes, updates or inserts rows of the same table leaves the graph matching it, "
          "whatever the declared columns are named")


def test_repeated_table(admin):
    # One table declared under two labels with different keys: each row is a
    # vertex under both.
    admin.run("CREATE TABLE hydradb_test.dual (id integer PRIMARY KEY, badge integer NOT NULL UNIQUE)")
    admin.run("INSERT INTO hydradb_test.dual VALUES (1, 101)")
    admin.run("""SELECT hydradb.create_graph('dual',
        '[{"label": "employee", "table": "hydradb_test.dual", "key": "id"},
          {"label": "badge", "table": "hydradb_test.dual", "key": "badge"}]')""")
    with Session() as reader:
        for sql, seeds in (
            ("SELECT 1", [{"employee": 1}, {"badge": 101}]),
            ("INSERT INTO hydradb_test.dual VALUES (2, 102)", [{"employee": 2}, {"badge": 102}]),
            ("UPDATE hydradb_test.dual SET badge = 202 WHERE id = 2", [{"employee": 1}, {"badge": 202}]),
            ("DELETE FROM hydradb_test.dual WHERE id = 1", [{"employee": 2}, {"badge": 202}]),
        ):
            admin.run(sql)
            adjacency_matches_sources(admin, "dual")
            for seed in seeds:
                by_key(reader, "reachable_vertices", seed, graph="dual")
        by_key(reader, "reachable_vertices", {"employee": 202}, graph="dual", error="missing vertex employee 202")
        by_key(reader, "reachable_vertices", {"badge": 102}, graph="dual", error="missing vertex badge 102")
    print("a table declared under two labels maintains each label from its own key")


def test_maintenance_random(admin):
    admin.run("CREATE TABLE hydradb_test.rnd_a (id integer PRIMARY KEY, s text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.rnd_b (id text PRIMARY KEY)")
    admin.run("CREATE TABLE hydradb_test.rnd_e (id bigint PRIMARY KEY, src integer NOT NULL REFERENCES hydradb_test.rnd_a ON DELETE CASCADE ON UPDATE CASCADE, "
              "dst text NOT NULL REFERENCES hydradb_test.rnd_b ON DELETE CASCADE, kind text NOT NULL)")
    admin.run("CREATE TABLE hydradb_test.rnd_k (id bigint PRIMARY KEY, x integer NOT NULL REFERENCES hydradb_test.rnd_a ON DELETE CASCADE ON UPDATE CASCADE, "
              "y integer NOT NULL REFERENCES hydradb_test.rnd_a ON DELETE CASCADE ON UPDATE CASCADE)")
    admin.run("INSERT INTO hydradb_test.rnd_a SELECT n, chr(97 + n % 7) FROM generate_series(1, 12) n")
    admin.run("INSERT INTO hydradb_test.rnd_b SELECT 'b' || n FROM generate_series(1, 6) n")
    admin.run("""SELECT hydradb.create_graph('rnd',
        '[{"label": "a", "table": "hydradb_test.rnd_a", "key": "id", "sort_key": "s"},
          {"label": "b", "table": "hydradb_test.rnd_b", "key": "id"}]',
        '[{"label": "e", "table": "hydradb_test.rnd_e", "key": "id", "kind": "kind",
           "source": "src", "source_label": "a", "target": "dst", "target_label": "b"},
          {"label": "k", "table": "hydradb_test.rnd_k", "key": "id",
           "source": "x", "source_label": "a", "target": "y", "target_label": "a"}]')""")
    rng = random.Random(267)
    ids = lambda table: [int(v) if table != "rnd_b" else v for v in admin.run(f"SELECT id FROM hydradb_test.{table} ORDER BY id")]
    next_id = [1000]

    def fresh():
        next_id[0] += 1
        return next_id[0]

    def statement():
        a, b = ids("rnd_a"), ids("rnd_b")
        e, k = ids("rnd_e"), ids("rnd_k")
        choices = [
            lambda: f"INSERT INTO hydradb_test.rnd_a VALUES ({fresh()}, '{rng.choice('abcdefg')}')",
            lambda: f"INSERT INTO hydradb_test.rnd_b VALUES ('b{fresh()}')",
        ]
        if a and b:
            choices.append(lambda: f"INSERT INTO hydradb_test.rnd_e VALUES ({fresh()}, {rng.choice(a)}, '{rng.choice(b)}', '{rng.choice('xyz')}')")
        if a:
            choices += [
                lambda: f"INSERT INTO hydradb_test.rnd_k VALUES ({fresh()}, {rng.choice(a)}, {rng.choice(a)})",
                lambda: f"UPDATE hydradb_test.rnd_a SET s = '{rng.choice('abcdefg')}' WHERE id = {rng.choice(a)}",
                lambda: f"UPDATE hydradb_test.rnd_a SET id = {fresh()} WHERE id = {rng.choice(a)}",
                lambda: f"DELETE FROM hydradb_test.rnd_a WHERE id = {rng.choice(a)}",
            ]
        if b:
            choices.append(lambda: f"DELETE FROM hydradb_test.rnd_b WHERE id = '{rng.choice(b)}'")
        if e and b:
            choices += [
                lambda: f"UPDATE hydradb_test.rnd_e SET kind = '{rng.choice('xyz')}', dst = '{rng.choice(b)}' WHERE id = {rng.choice(e)}",
                lambda: f"DELETE FROM hydradb_test.rnd_e WHERE id = {rng.choice(e)}",
            ]
        if k:
            choices.append(lambda: f"DELETE FROM hydradb_test.rnd_k WHERE id = {rng.choice(k)}")
        return rng.choice(choices)()

    def expected_reach(seed, max_depth):
        """Every vertex within max_depth of seed, both directions, with its
        hop distance, from the source tables alone."""
        neighbours = {}
        for src, dst in (row.split("|") for row in admin.run("SELECT src || '|' || dst FROM hydradb_test.rnd_e")):
            neighbours.setdefault(("a", int(src)), set()).add(("b", dst))
            neighbours.setdefault(("b", dst), set()).add(("a", int(src)))
        for x, y in (row.split("|") for row in admin.run("SELECT x || '|' || y FROM hydradb_test.rnd_k")):
            neighbours.setdefault(("a", int(x)), set()).add(("a", int(y)))
            neighbours.setdefault(("a", int(y)), set()).add(("a", int(x)))
        start = next(iter(seed.items()))
        depths, frontier = {start: 0}, [start]
        for depth in range(1, max_depth + 1):
            reached = {n for v in frontier for n in neighbours.get(v, ()) if n not in depths}
            depths.update(dict.fromkeys(reached, depth))
            frontier = list(reached)
        return {(label, key, depth) for (label, key), depth in depths.items()}

    compared_calls = 0
    with Session() as reader:
        for _ in range(60):
            admin.run("BEGIN; " + "; ".join(statement() for _ in range(rng.randint(1, 3))) + "; COMMIT")
            adjacency_matches_sources(admin, "rnd")
            a = ids("rnd_a")
            seeds = [{"a": v} for v in rng.sample(a, min(2, len(a)))] + [{"b": v} for v in ids("rnd_b")[:1]]
            for seed in seeds:
                # An answer computed from the source tables alone.
                reached = by_key(reader, "reachable_vertices", seed, options={"direction": "both", "max_depth": 4}, graph="rnd")
                actual = {(item["label"], item["key"], item["depth"]) for item in reached["items"]}
                require(actual == expected_reach(seed, 4), (seed, actual ^ expected_reach(seed, 4)))
                compared_calls += 1
                for operation in ("reachable_vertices", "reachable_edges"):
                    # With limits that cut layers.
                    for caps in ({"max_vertices": 3, "max_edges": 2}, {"max_vertices": 6, "max_edges": 5}):
                        by_key(reader, operation, seed, options={"direction": "both", "max_depth": 4, **caps}, graph="rnd")
            if len(a) >= 2:
                for options in ({"edge_kinds": ["k"], "direction": "both", "max_depth": 3, "paths_per_pair": 0},
                                {"edge_kinds": ["k", "x", "y"], "paths_per_pair": 2}):
                    by_key(reader, "paths_from", {"a": a[0]}, options=options, graph="rnd")
                    by_key(reader, "paths_between", {"a": a[:3]}, {"a": a[-3:]}, options=options, graph="rnd")
    print(f"randomized writes across four tables with cascades: adjacency tables mirror the sources, kinds and sort keys included, "
          f"and {compared_calls} walks match an answer computed from the source tables")


def test_drop_graph(admin):
    create_tables(admin, "hydradb_test.drop_vertices", "hydradb_test.drop_edges")
    admin.run("INSERT INTO hydradb_test.drop_vertices VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    admin.run("INSERT INTO hydradb_test.drop_edges VALUES (1, 1, 2, 'link')")
    register = single_table_graph("dropped", "hydradb_test.drop_vertices", "hydradb_test.drop_edges")
    triggers = ("SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'pg_hydradb_' || md5('dropped') || '\\_%' "
                "AND tgrelid IN ('hydradb_test.drop_vertices'::regclass, 'hydradb_test.drop_edges'::regclass)")
    catalog = ("SELECT (SELECT count(*) FROM hydradb.graphs WHERE name = 'dropped') + "
               "(SELECT count(*) FROM hydradb.graph_vertex_tables WHERE graph = 'dropped') + "
               "(SELECT count(*) FROM hydradb.graph_edge_tables WHERE graph = 'dropped')")
    orphans = ("SELECT (SELECT count(*) FROM hydradb.graph_vertices WHERE graph NOT IN (SELECT registration FROM hydradb.graphs)) + "
               "(SELECT count(*) FROM hydradb.graph_adjacency WHERE graph NOT IN (SELECT registration FROM hydradb.graphs))")
    admin.run(register)
    require(admin.run(triggers) == ["8"], "create_graph did not install four triggers per table")
    with Session() as reader:
        require(ids(query(reader, graph="dropped")) == [1, 2], "dropped graph initial load failed")
        admin.run("INSERT INTO hydradb_test.drop_edges VALUES (2, 2, 3, 'link')")

        # Dropping removes the triggers and every catalog and adjacency row, and keeps the tables.
        admin.run("SELECT hydradb.drop_graph('dropped')")
        require(admin.run(triggers) == ["0"], "drop_graph left triggers")
        require(admin.run(catalog) == ["0"] and admin.run(orphans) == ["0"], "drop_graph left catalog or adjacency rows")
        reader.run(query_sql(graph="dropped"), error="unknown graph")
        admin.run("INSERT INTO hydradb_test.drop_edges VALUES (3, 3, 1, 'link')")
        require(admin.run("SELECT count(*) FROM hydradb_test.drop_edges") == ["3"], "tables lost rows or rejected writes")
        admin.run("SELECT hydradb.drop_graph('dropped')", error="unknown graph dropped")

        admin.run(register)
        require(ids(query(reader, graph="dropped")) == [1, 2, 3], "re-registered graph missed rows")

        # A graph row deleted by hand leaves triggers that fail every write;
        # drop_graph finds them by name and removes them and the orphaned rows.
        admin.run("DELETE FROM hydradb.graphs WHERE name = 'dropped'")
        admin.run("INSERT INTO hydradb_test.drop_edges VALUES (4, 1, 3, 'link')", error="graph registration is not visible")
        admin.run("SELECT hydradb.drop_graph('dropped')")
        require(admin.run(triggers) == ["0"] and admin.run(orphans) == ["0"], "drop_graph left a deleted graph's triggers or rows")
        admin.run("INSERT INTO hydradb_test.drop_edges VALUES (4, 1, 3, 'link')")

        # A write and a drop in one transaction commit together.
        admin.run(register)
        admin.run("BEGIN; INSERT INTO hydradb_test.drop_edges VALUES (5, 2, 1, 'link'); "
                  "SELECT hydradb.drop_graph('dropped'); COMMIT")
        require(admin.run("SELECT count(*) FROM hydradb_test.drop_edges") == ["5"], "write and drop did not commit together")
        require(admin.run(triggers) == ["0"] and admin.run(catalog) == ["0"] and admin.run(orphans) == ["0"],
                "write then drop left triggers, catalog or adjacency rows")
    with Session() as stranger:
        stranger.run("SET ROLE hydradb_test_reader")
        stranger.run("SELECT hydradb.drop_graph('chain')", error="permission denied")

    # A trigger of the user's that happens to carry one of a graph's trigger
    # names is neither replaced by create_graph nor dropped by drop_graph.
    prefix = "pg_hydradb_" + hashlib.md5(b"collide").hexdigest()
    admin.run("CREATE FUNCTION hydradb_test.audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$")
    user_trigger = ("SELECT tgfoid::regprocedure FROM pg_trigger WHERE tgrelid = 'hydradb_test.drop_edges'::regclass "
                    f"AND tgname = '{prefix}_t'")
    admin.run(f"CREATE TRIGGER {prefix}_t AFTER TRUNCATE ON hydradb_test.drop_edges FOR EACH STATEMENT EXECUTE FUNCTION hydradb_test.audit()")
    collide = single_table_graph("collide", "hydradb_test.drop_vertices", "hydradb_test.drop_edges")
    admin.run(collide, error="does not belong to pg_hydradb")
    require(admin.run("SELECT count(*) FROM hydradb.graphs WHERE name = 'collide'") == ["0"], "refused registration left a graph row")
    require(admin.run(user_trigger) == ["hydradb_test.audit()"], "create_graph replaced the user's trigger")
    # Registered while the name is free, then the user swaps one of the graph's
    # triggers for their own of the same name.
    admin.run(f"DROP TRIGGER {prefix}_t ON hydradb_test.drop_edges")
    admin.run(collide)
    admin.run(f"DROP TRIGGER {prefix}_t ON hydradb_test.drop_edges")
    admin.run(f"CREATE TRIGGER {prefix}_t AFTER TRUNCATE ON hydradb_test.drop_edges FOR EACH STATEMENT EXECUTE FUNCTION hydradb_test.audit()")
    admin.run("SELECT hydradb.drop_graph('collide')")
    require(admin.run(user_trigger) == ["hydradb_test.audit()"], "drop_graph dropped the user's trigger")
    require(admin.run(f"SELECT count(*) FROM pg_trigger WHERE tgname LIKE '{prefix}%'") == ["1"], "drop_graph left pg_hydradb triggers")
    admin.run(f"DROP TRIGGER {prefix}_t ON hydradb_test.drop_edges")
    print("drop_graph removes triggers, catalog and adjacency rows, keeps tables, cleans up hand-deleted graphs, and re-registration refills; "
          "neither create_graph nor drop_graph touches a same-named trigger that is not pg_hydradb's")


def test_admin_locks_all_or_none(admin):
    """create_graph, refresh_graph and drop_graph take their tables' locks all
    at once: they never hold one table while waiting for another, so a writer
    that writes the tables in another order neither deadlocks with them nor
    queues behind them."""
    create_tables(admin, "hydradb_test.lk_vertices", "hydradb_test.lk_edges")
    admin.run("INSERT INTO hydradb_test.lk_vertices SELECT n, n::text FROM generate_series(1, 4) n")
    admin.run(single_table_graph("locks", "hydradb_test.lk_vertices", "hydradb_test.lk_edges"))
    with Session() as writer:
        # The writer holds lk_edges; refresh_graph wants both tables; then the
        # writer writes lk_vertices. Taking lk_vertices alone first, as before,
        # left the writer waiting for refresh_graph and refresh_graph for the
        # writer.
        writer.run("BEGIN; INSERT INTO hydradb_test.lk_edges VALUES (1, 1, 2, 'link')")
        outcome = {}

        def refresh():
            try:
                admin.run("SELECT hydradb.refresh_graph('locks')")
            except AssertionError as error:
                outcome["error"] = error

        thread = threading.Thread(target=refresh)
        thread.start()
        time.sleep(0.3)
        started = time.monotonic()
        writer.run("INSERT INTO hydradb_test.lk_vertices VALUES (5, '5')")
        waited = time.monotonic() - started
        writer.run("COMMIT")
        thread.join()
        require("error" not in outcome, outcome)
        require(waited < 0.5, f"the writer waited {waited:.2f}s behind refresh_graph")
        adjacency_matches_sources(admin, "locks")
        # A table that stays busy past lock_timeout is a clear, retryable error.
        writer.run("BEGIN; INSERT INTO hydradb_test.lk_edges VALUES (2, 2, 3, 'link')")
        admin.run("SET lock_timeout = '300ms'")
        for call in ("SELECT hydradb.refresh_graph('locks')", "SELECT hydradb.drop_graph('locks')",
                     single_table_graph("locks_again", "hydradb_test.lk_vertices", "hydradb_test.lk_edges")):
            admin.run(call, error="could not lock graph source hydradb_test.lk_edges")
        admin.run("SET lock_timeout = '5s'")
        writer.run("ROLLBACK")
    admin.run("SELECT hydradb.drop_graph('locks')")
    print("create_graph, refresh_graph and drop_graph lock all their tables at once and give up at lock_timeout")


def test_concurrent_writers(admin):
    create_tables(admin, "hydradb_test.ct_vertices", "hydradb_test.ct_edges")
    admin.run("CREATE TABLE hydradb_test.ct_accounts (id integer PRIMARY KEY, balance integer NOT NULL)")
    admin.run("INSERT INTO hydradb_test.ct_accounts VALUES (7, 100)")
    admin.run("INSERT INTO hydradb_test.ct_vertices SELECT n, n::text FROM generate_series(1, 6) n")
    admin.run(single_table_graph("commit_time", "hydradb_test.ct_vertices", "hydradb_test.ct_edges"))
    with Session() as first, Session() as second, Session() as reader:
        # First writes the graph and then wants an account; second holds the
        # account and then writes the graph. Writers of different edges share
        # no rows, so second does not wait for first.
        first.run("BEGIN; INSERT INTO hydradb_test.ct_edges VALUES (11, 1, 2, 'link')")
        second.run("SET lock_timeout = '1s'")
        second.run("BEGIN; UPDATE hydradb_test.ct_accounts SET balance = balance - 1 WHERE id = 7")
        second.run("INSERT INTO hydradb_test.ct_edges VALUES (12, 2, 3, 'link')")
        second.run("COMMIT")
        first.run("UPDATE hydradb_test.ct_accounts SET balance = balance + 1 WHERE id = 7")
        first.run("COMMIT")
        require(ids(query(reader, graph="commit_time", options={"max_depth": 10})) == [1, 2, 3], "concurrent writers' edges are missing")
        # Under stricter isolation, writers of different edges of one graph do
        # not conflict either.
        for isolation in ("REPEATABLE READ", "SERIALIZABLE"):
            first.run(f"BEGIN ISOLATION LEVEL {isolation}; INSERT INTO hydradb_test.ct_edges VALUES (21, 3, 4, 'link')")
            second.run(f"BEGIN ISOLATION LEVEL {isolation}; INSERT INTO hydradb_test.ct_edges VALUES (22, 4, 5, 'link')")
            second.run("COMMIT")
            first.run("COMMIT")
            require(ids(query(reader, graph="commit_time", options={"max_depth": 10})) == [1, 2, 3, 4, 5],
                    f"{isolation}: concurrent writers' edges are missing")
            admin.run("DELETE FROM hydradb_test.ct_edges WHERE id IN (21, 22)")
        admin.run("DELETE FROM hydradb_test.ct_edges WHERE id IN (11, 12)")
        adjacency_matches_sources(admin, "commit_time")
        # Several statements of one transaction on the same edge key.
        admin.run("BEGIN; INSERT INTO hydradb_test.ct_edges VALUES (5, 1, 4, 'link'); "
                  "UPDATE hydradb_test.ct_edges SET target = 5 WHERE id = 5; "
                  "DELETE FROM hydradb_test.ct_edges WHERE id = 5; "
                  "INSERT INTO hydradb_test.ct_edges VALUES (5, 1, 6, 'link'); "
                  "UPDATE hydradb_test.ct_edges SET id = 6 WHERE id = 5; COMMIT")
        adjacency_matches_sources(admin, "commit_time")
        require(ids(query(reader, graph="commit_time")) == [1, 6], "statement sequence on one key went wrong")
    print("writers of different edges do not wait for each other at any isolation level; statement sequences on one key are maintained")


def test_dump_restore(admin):
    psql = Path(os.environ["PGHYDRADB_TEST_PSQL"])
    dump = Path(os.environ["PGHYDRADB_TEST_ROOT"]) / "dump.sql"
    admin.run("CREATE DATABASE hydradb_dump_source")
    admin.run("CREATE DATABASE hydradb_dump_target")
    with Session("hydradb_dump_source") as source:
        source.run("CREATE EXTENSION pg_hydradb")
        source.run("CREATE SCHEMA app")
        source.run("CREATE TABLE app.people (id integer PRIMARY KEY, name text NOT NULL)")
        source.run("CREATE TABLE app.companies (slug text PRIMARY KEY)")
        source.run("CREATE TABLE app.jobs (id bigint PRIMARY KEY, person_id integer NOT NULL REFERENCES app.people, "
                   "company_slug text NOT NULL REFERENCES app.companies)")
        source.run("INSERT INTO app.people VALUES (1, 'ann'), (2, 'bob')")
        source.run("INSERT INTO app.companies VALUES ('acme')")
        source.run("INSERT INTO app.jobs VALUES (1, 1, 'acme')")
        source.run("""SELECT hydradb.create_graph('jobs',
            '[{"label": "person", "table": "app.people", "key": "id", "sort_key": "name"},
              {"label": "company", "table": "app.companies", "key": "slug"}]',
            '[{"label": "works_at", "table": "app.jobs", "key": "id",
               "source": "person_id", "source_label": "person", "target": "company_slug", "target_label": "company"}]')""")
        # A dropped graph moves the registration sequence past the live one.
        source.run("""SELECT hydradb.create_graph('gone', '[{"label": "person", "table": "app.people", "key": "id"}]')""")
        source.run("SELECT hydradb.drop_graph('gone')")
        source.run("INSERT INTO app.jobs VALUES (2, 2, 'acme')")
        before = normalized(by_key(source, "reachable_vertices", {"company": "acme"}, options={"direction": "incoming"}, graph="jobs"))
        registered = source.run("SELECT registration FROM hydradb.graphs WHERE name = 'jobs'")
    subprocess.run([str(psql.with_name("pg_dump")), "-d", "hydradb_dump_source", "-f", str(dump)], check=True)
    subprocess.run([str(psql), "-X", "-q", "-v", "ON_ERROR_STOP=1", "-d", "hydradb_dump_target", "-f", str(dump)],
                   check=True, stdout=subprocess.DEVNULL)
    with Session("hydradb_dump_target") as target:
        target.run("CREATE SCHEMA hydradb_test")
        install_checks(target)
        require(target.run("SELECT registration FROM hydradb.graphs WHERE name = 'jobs'") == registered,
                "restore lost the graph registration")
        require(target.run("SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'pg_hydradb_%'") == ["12"], "restore lost triggers")
        adjacency_matches_sources(target, "jobs")
        after = by_key(target, "reachable_vertices", {"company": "acme"}, options={"direction": "incoming"}, graph="jobs")
        require(after["items"] == before["items"], after)
        # The restored triggers find their graph: writes succeed and are maintained.
        target.run("INSERT INTO app.people VALUES (3, 'cat'); INSERT INTO app.jobs VALUES (3, 3, 'acme')")
        adjacency_matches_sources(target, "jobs")
        target.run("""SELECT hydradb.create_graph('later', '[{"label": "person", "table": "app.people", "key": "id"}]')""")
        require(target.run("SELECT registration FROM hydradb.graphs WHERE name = 'later'") == ["3"], "registration sequence was not restored")
        adjacency_matches_sources(target, "later")
        # A graph the dump does not know keeps its triggers through a --clean
        # restore, so the restore could not drop its tables.
        target.run("SELECT hydradb.drop_graph('later')")
    # A --clean restore over a live database drops the graph triggers before
    # the constraints and tables they guard, so none of those drops is refused.
    subprocess.run([str(psql.with_name("pg_dump")), "--clean", "--if-exists", "-d", "hydradb_dump_source", "-f", str(dump)], check=True)
    subprocess.run([str(psql), "-X", "-q", "-v", "ON_ERROR_STOP=1", "-d", "hydradb_dump_target", "-f", str(dump)],
                   check=True, stdout=subprocess.DEVNULL)
    with Session("hydradb_dump_target") as target:
        require(target.run("SELECT name FROM hydradb.graphs ORDER BY name") == ["jobs"], "a --clean restore kept the wrong graphs")
        adjacency_matches_sources(target, "jobs")
        target.run("DROP EXTENSION pg_hydradb CASCADE")
        require(target.run("SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'pg_hydradb_%'") == ["0"], "DROP EXTENSION left triggers")
    admin.run("DROP DATABASE hydradb_dump_source")
    admin.run("DROP DATABASE hydradb_dump_target")
    print("pg_dump/restore keeps graph registrations, the adjacency tables and the sequences; restored triggers maintain writes; "
          "a --clean restore and DROP EXTENSION CASCADE pass")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--recovery", action="store_true")
    arguments = parser.parse_args()
    if arguments.recovery:
        test_recovery()
        return
    with Session() as reader, Session() as writer:
        install_checks(writer)
        test_queries(reader)
        test_limits(reader)
        test_mutations(reader, writer)
        test_own_writes(writer)
        test_replica_role_writes(writer)
        test_streamed_snapshot_budget(writer)
        test_snapshot_field_limits(writer)
        test_permissions(writer)
        test_registration_owner(writer)
        test_cached_definitions(writer)
        test_refresh_restores_triggers(writer)
        test_registration_race(writer)
        test_cast_sensitive_sources(writer)
        test_seed_order(writer)
        test_update_comparison(writer)
        test_path_work_limit(writer)
        test_incoming_path_hub(writer)
        test_column_rewrite_flagged(writer)
        test_isolation_and_columns(writer)
        test_declared_columns_fixed(writer)
        test_long_keys(writer)
        test_hub_layer(writer)
        test_mirrorable(writer)
        test_views_rejected(writer)
        test_volatile_policy_write(writer)
        test_sources_and_key_types(writer)
        test_result_names_charged(writer)
        test_multi_table(writer)
        test_maintenance(writer)
        test_user_after_triggers(writer)
        test_repeated_table(writer)
        test_maintenance_random(writer)
        test_drop_graph(writer)
        test_concurrent_writers(writer)
        test_admin_locks_all_or_none(writer)
        test_dump_restore(writer)
        test_concurrency(writer)


if __name__ == "__main__":
    main()
