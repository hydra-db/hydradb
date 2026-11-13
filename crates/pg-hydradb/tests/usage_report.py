"""The anonymous usage report, against the server test_pg_hydradb.sh
started, after integration.py has run on it.

1. Without preloading: hydradb.usage_report() works, has no activity, and no
   worker runs.
2. Preloaded, reporting to a local stub: the worker sends reports with every
   database that has pg_hydradb and the counters the session's calls and
   refusals left, and no name, key or query text.
3. A lock held on one database's graph catalog: the report still goes out,
   with that database counted unread.
4. With pg_hydradb.usage_report = off: no worker and nothing sent.
"""

import http.server
import json
import os
import subprocess
import threading
import time
from pathlib import Path

from integration import Session, literal, query_sql, require

ROOT = Path(os.environ["PGHYDRADB_TEST_ROOT"])
PG_CTL = os.environ["PGHYDRADB_TEST_PG_CTL"]
CONF = ROOT / "data" / "postgresql.conf"
OTHER_DATABASE = "hydradb_usage_other"
PLAIN_DATABASE = "hydradb_usage_plain"
MANY_DATABASE = "hydradb_usage_many"
# The report lists at most this many graphs per database, and counts the rest.
MAX_GRAPHS_PER_DATABASE = 1024

QUERY_FUNCTIONS = {"reachable_vertices", "reachable_edges", "paths_from", "paths_between"}
# Names the fixture and this test give to schemas, tables, columns, labels,
# graphs and values. None may appear in a report.
PRIVATE = ["hydradb_test", "hydradb_usage", "chain", "mutable", "mvcc", "sort_key", "link", "secret_label",
           "usage_graph", "usage_people", "usage_knows", "usage_many", OTHER_DATABASE, PLAIN_DATABASE, MANY_DATABASE]


class Receiver:
    """Accepts POST /v1/pg-usage on 127.0.0.1 and keeps every body."""

    def __init__(self):
        self.reports = []
        self.paths = []
        receiver = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers["content-length"]))
                receiver.paths.append(self.path)
                receiver.reports.append((body.decode(), self.headers.get("content-type")))
                self.send_response(202)
                self.end_headers()

            def log_message(self, *_):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def wait_for(self, predicate, timeout=60, since=0):
        """The first report, from the `since`-th received on, that matches."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            for body, _ in list(self.reports)[since:]:
                report = json.loads(body)
                if predicate(report):
                    return report
            time.sleep(0.2)
        raise AssertionError(f"no matching report within {timeout}s; got {len(self.reports)}: {self.reports[-1:]}")

    def close(self):
        self.server.shutdown()


def set_conf(settings):
    """Replaces this test's block at the end of postgresql.conf."""
    marker = "# usage_report.py\n"
    text = CONF.read_text().split(marker)[0]
    CONF.write_text(text + marker + "".join(f"{key} = {value}\n" for key, value in settings.items()))


def restart():
    subprocess.run([PG_CTL, "-D", str(ROOT / "data"), "-l", str(ROOT / "server.log"), "-m", "fast", "-w", "restart"],
                   check=True, stdout=subprocess.DEVNULL)


def workers(session):
    return session.run("SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'pg_hydradb usage report'")[0]


def check_shape(report):
    require(report["version"] == 1, report)
    for field in ("report_id", "install_id"):
        require(len(report[field]) == 32 and all(c in "0123456789abcdef" for c in report[field]), report[field])
    require(report["build"]["postgres_major"] >= 16, report["build"])
    text = json.dumps(report)
    for name in PRIVATE:
        require(name not in text, f"{name!r} leaked into the report: {text}")


def check_without_preloading():
    with Session() as admin:
        report = admin.json("SELECT hydradb.usage_report()")
        check_shape(report)
        require(report["activity"] is None, "activity without shared memory")
        require(len(report["databases"]) == 1, report["databases"])
        graphs = report["databases"][0]["graphs"]
        require(len(graphs) >= 3 and all(g["vertex_tables"] >= 1 for g in graphs), graphs)
        require(report["databases"][0]["graphs_omitted"] == 0, report["databases"][0])
        require(workers(admin) == "0", "a worker runs without preloading")
        # Counting is a no-op, and harmless, without shared memory.
        admin.run("SELECT hydradb.usage_count('create_graph'), hydradb.usage_count('not a code')")
        # A role that is not the owner may not read it.
        admin.run("SET ROLE hydradb_test_reader")
        admin.run("SELECT hydradb.usage_report()", error="permission denied")
        # Nor invent counts.
        admin.run("SELECT hydradb.usage_count('create_graph')", error="permission denied")
        admin.run("RESET ROLE")
        install_id = report["install_id"]
        require(admin.json("SELECT hydradb.usage_report()")["install_id"] == install_id, "install ID changed")
        return install_id


def check_graphs_past_the_limit():
    """A database with more graphs than a report lists counts the rest in
    graphs_omitted."""
    with Session() as admin:
        admin.run(f"DROP DATABASE IF EXISTS {MANY_DATABASE}")
        admin.run(f"CREATE DATABASE {MANY_DATABASE}")
    try:
        with Session(MANY_DATABASE) as many:
            many.run("CREATE EXTENSION pg_hydradb")
            # Registering this many graphs through create_graph takes minutes,
            # since each one re-checks the triggers of every graph before it.
            # The report reads only the catalog, so fill it directly.
            many.run(f"INSERT INTO hydradb.graphs (name, indexed) "
                     f"SELECT 'usage_many_' || n, false FROM generate_series(1, {MAX_GRAPHS_PER_DATABASE + 2}) n")
            database = many.json("SELECT hydradb.usage_report()")["databases"][0]
            require(len(database["graphs"]) == MAX_GRAPHS_PER_DATABASE, len(database["graphs"]))
            require(database["graphs_omitted"] == 2, database["graphs_omitted"])
            check_shape(many.json("SELECT hydradb.usage_report()"))
    finally:
        with Session() as admin:
            admin.run(f"DROP DATABASE IF EXISTS {MANY_DATABASE}")


def other_databases():
    with Session() as admin:
        admin.run(f"DROP DATABASE IF EXISTS {OTHER_DATABASE}")
        admin.run(f"DROP DATABASE IF EXISTS {PLAIN_DATABASE}")
        admin.run(f"CREATE DATABASE {OTHER_DATABASE}")
        admin.run(f"CREATE DATABASE {PLAIN_DATABASE}")
    with Session(OTHER_DATABASE) as other:
        other.run("CREATE EXTENSION pg_hydradb")
        other.run("CREATE TABLE usage_people (id bigint PRIMARY KEY)")
        other.run("CREATE TABLE usage_knows (id bigint PRIMARY KEY, a bigint NOT NULL REFERENCES usage_people, "
                  "b bigint NOT NULL REFERENCES usage_people)")
        other.run("INSERT INTO usage_people SELECT generate_series(1, 20)")
        other.run("INSERT INTO usage_knows SELECT n, n, n + 1 FROM generate_series(1, 19) n")
        vertices = [{"label": "secret_label", "table": "public.usage_people", "key": "id"}]
        edges = [{"label": "usage_knows", "table": "public.usage_knows", "key": "id", "source": "a",
                  "source_label": "secret_label", "target": "b", "target_label": "secret_label"}]
        other.run(f"SELECT hydradb.create_graph('usage_graph', {literal(json.dumps(vertices))}, "
                  f"{literal(json.dumps(edges))})")


def activity_calls(session):
    """Calls and refusals the preloaded server must count."""
    for _ in range(3):
        session.run(query_sql(seeds=(1,), options={"max_depth": 3}))
    session.run(query_sql(seeds=(1,), operation="reachable_edges", options={"max_depth": 1}))
    session.run(query_sql(seeds=(1,), operation="paths_between", targets=(4,)))
    # Truncated by the caller's own depth.
    session.run(query_sql(seeds=(1,), options={"max_depth": 1}))
    session.run(query_sql(graph="no_such_graph"), error="unknown graph")
    session.run(query_sql(seeds=(999,)), error="missing vertex")
    session.run(query_sql(options={"max_depth": 1000}), error="max_depth")
    # create_graph refuses a nullable key, and the count survives the rollback.
    session.run("CREATE TABLE hydradb_test.usage_nullable (id bigint UNIQUE)")
    session.run("SELECT hydradb.create_graph('usage_nullable', "
                + literal(json.dumps([{"label": "v", "table": "hydradb_test.usage_nullable", "key": "id"}]))
                + ")", error="allows NULL")
    session.run("DROP TABLE hydradb_test.usage_nullable")


def check_preloaded(install_id):
    receiver = Receiver()
    try:
        set_conf({
            "shared_preload_libraries": "'pg_hydradb'",
            "pg_hydradb.usage_report_url": f"'http://127.0.0.1:{receiver.port}/v1/pg-usage'",
            "pg_hydradb.usage_report_interval": "2",
        })
        restart()
        log = (ROOT / "server.log").read_text()
        require("set pg_hydradb.usage_report = off to stop" in log, "no startup line saying how to turn it off")
        with Session() as admin:
            require(workers(admin) == "1", "the reporting worker is not running")
            activity_calls(admin)
            local = admin.json("SELECT hydradb.usage_report()")
            require(local["activity"] is not None, "activity missing with preloading")

        def counted(report):
            activity = report.get("activity") or {}
            queries = activity.get("queries", {})
            return (queries.get("reachable_vertices", {}).get("calls", 0) >= 6
                    and activity.get("failures", {}).get("nullable_column", 0) >= 1)

        report = receiver.wait_for(counted)
        check_shape(report)
        require(set(receiver.paths) == {"/v1/pg-usage"}, receiver.paths)
        require(all(content_type == "application/json" for _, content_type in receiver.reports), "content type")
        require(report["install_id"] == install_id, "the worker's install ID differs from usage_report()'s")
        require(report["databases_unread"] == 0, report)
        # The fixture database and the other one; not the one without pg_hydradb.
        databases = report["databases"]
        require(len(databases) == 2, databases)
        require(sorted(len(d["graphs"]) for d in databases)[0] == 1, databases)

        activity = report["activity"]
        require(set(activity["queries"]) == QUERY_FUNCTIONS, activity["queries"].keys())
        vertices = activity["queries"]["reachable_vertices"]
        require(vertices["failures"] == 3, vertices)
        require(sum(vertices["latency_ms"]) == vertices["calls"], vertices)
        require(sum(vertices["hops"]) == vertices["calls"] - vertices["failures"], vertices)
        require(activity["queries"]["paths_between"]["calls"] == 1, activity["queries"]["paths_between"])
        failures = activity["failures"]
        for code in ("unknown_graph", "missing_seed", "invalid_options", "nullable_column"):
            require(failures.get(code, 0) >= 1, f"{code} not counted: {failures}")
        require(activity["truncations"].get("depth", 0) >= 1, activity["truncations"])
        require(activity["admin"].get("create_graph", 0) >= 1, activity["admin"])
        require(report["server_started_at_unix_ms"] <= report["sent_at_unix_ms"], report)
        print(f"usage report: {len(receiver.reports)} report(s) received, {len(json.dumps(report))} bytes")
    finally:
        receiver.close()


def check_locked():
    """A lock held on a database's graph catalog does not stop the report:
    that database's read gives up at its lock_timeout and is counted in
    databases_unread, and the next report after the lock goes reads it again."""
    receiver = Receiver()
    try:
        set_conf({
            "shared_preload_libraries": "'pg_hydradb'",
            "pg_hydradb.usage_report_url": f"'http://127.0.0.1:{receiver.port}/v1/pg-usage'",
            "pg_hydradb.usage_report_interval": "2",
        })
        restart()
        with Session(OTHER_DATABASE) as holder:
            holder.run("BEGIN; LOCK TABLE hydradb.graphs IN ACCESS EXCLUSIVE MODE")
            locked = receiver.wait_for(lambda r: r["databases_unread"] == 1 and len(r["databases"]) == 1)
            check_shape(locked)
            holder.run("ROLLBACK")
        # Only reports sent after the lock went: one from before the lock was
        # taken would also have both databases.
        receiver.wait_for(lambda r: r["databases_unread"] == 0 and len(r["databases"]) == 2,
                          since=len(receiver.reports))
        print("usage report: a locked database is counted unread and the report still goes out")
    finally:
        receiver.close()


def check_off():
    receiver = Receiver()
    try:
        set_conf({
            "shared_preload_libraries": "'pg_hydradb'",
            "pg_hydradb.usage_report": "off",
            "pg_hydradb.usage_report_url": f"'http://127.0.0.1:{receiver.port}/v1/pg-usage'",
            "pg_hydradb.usage_report_interval": "1",
        })
        restart()
        with Session() as admin:
            require(workers(admin) == "0", "a worker runs with usage_report off")
            admin.run(query_sql())
            # By hand it still works, and still counts.
            require(admin.json("SELECT hydradb.usage_report()")["activity"] is not None, "activity")
        time.sleep(4)
        require(not receiver.reports, f"sent with usage_report off: {receiver.reports}")
    finally:
        receiver.close()


def main():
    install_id = check_without_preloading()
    check_graphs_past_the_limit()
    other_databases()
    check_preloaded(install_id)
    check_locked()
    check_off()
    # Leave the server as the script started it.
    set_conf({})
    restart()
    with Session() as admin:
        admin.run(f"DROP DATABASE {OTHER_DATABASE}")
        admin.run(f"DROP DATABASE {PLAIN_DATABASE}")
    print("usage report tests passed")


if __name__ == "__main__":
    main()
