#!/usr/bin/env python3
"""Neo4j as an independent oracle for the read parity matrix.

`query-language/cypher/parity/legacy-read-parity.json` records, for every read shape, the
columns and rows HydraDB must return. Until now those expectations were checked
against HydraDB's own two engines only, so a value both engines got wrong in the
same way passed. This script checks the recorded expectations against a third
implementation that shares no code with either engine: Neo4j, running Cypher 25.

For every cell the fixture seeds, the graph is wiped and that cell's vertices and
edges are written to Neo4j. Every matrix row for the cell then runs as
`CYPHER 25 <query>` with the row's parameters, and the answer is compared with
the row's recorded `columns` and `rows` under the row's own ordering contract.

What the recorded expectation means for each kind of row:

* A row with a recorded result -- every supported row, and every gap row whose
  target answer has been recorded -- must come back from Neo4j with exactly that
  result. A difference is a finding about the matrix or about HydraDB, never a
  reason to edit the row until it matches.
* A row carrying `"neo4j": {"expect": "error", ...}` records a shape Neo4j also
  refuses, such as `WITH e.entity_id` without an alias. Neo4j must refuse it.
* A row listed in `query-language/cypher/parity/neo4j-divergences.json` is a known, argued
  difference between HydraDB and Neo4j. It may differ, but each entry carries a
  reason and a reviewer, and an entry that starts agreeing again fails the run,
  so the allowlist cannot quietly outlive the difference it excuses.
* An unsupported row with no recorded result is a gap nobody has specified yet.
  The run prints Neo4j's answer for it, which is the starting point for the row,
  not a substitute for reviewing it.

Identity: HydraDB reads `n.id` and `{id: ...}` as the vertex's graph identity.
The seeder stores that identity as an `id` property, so both spellings mean the
same thing on both systems, and a node Neo4j returns is compared by that
property. HydraDB keys an edge on `(type, source, target)`, so the fixture never
holds two parallel edges of one type; Neo4j would allow them, and the seeder
refuses a fixture that relies on it.

Usage:

    # Offline: normalisation, comparison and fixture checks. No Neo4j needed.
    python3 query-language/cypher/scripts/neo4j_parity_oracle.py self-test

    # Live: NEO4J_PASSWORD must hold the password; it is never printed.
    python3 query-language/cypher/scripts/neo4j_parity_oracle.py run --uri bolt://127.0.0.1:37687 \
        --jsonl logs/neo4j-oracle.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

PARITY_DIR = Path(__file__).resolve().parent.parent / "parity"
DEFAULT_MATRIX = PARITY_DIR / "legacy-read-parity.json"
DEFAULT_DIVERGENCES = PARITY_DIR / "neo4j-divergences.json"

# Orderings under which rows are compared by position; anything else is a
# multiset comparison. Only the orders Cypher itself guarantees: an ORDER BY on
# the result, one row, or none.
#
# Deliberately narrower than `MatrixRow::assert_rows_match` in
# src/shard/legacy_read_parity.rs, which also compares `union-arm-order`
# positionally. That is sound there, where both sides are HydraDB and run the
# arms the same way, but it is not a Cypher guarantee: rows inside a UNION ALL
# arm with no ORDER BY come back in whatever order the arm produces them, and
# Neo4j's order for the same arm changes between runs.
POSITIONAL_ORDERINGS = frozenset({"deterministic", "single-row", "none"})

# Verdicts that fail a run. Everything else is informational.
FAILING_VERDICTS = frozenset(
    {"mismatch", "unexpected-error", "unexpected-success", "stale-divergence",
    "refusal-mismatch",
    "divergence-kind-mismatch", "column-mismatch"}
)

FLOAT_RELATIVE_TOLERANCE = 1e-9


class OracleError(Exception):
    """A matrix or fixture this harness cannot honestly run."""


# ---------------------------------------------------------------------------
# Values
#
# Both sides are reduced to one canonical form before comparison: a tuple whose
# first element names the type. Integers and floats stay distinct -- `1` and
# `1.0` are different values to a Cypher client and to the matrix -- and a
# boolean is never an integer, which Python would otherwise let it be.
# ---------------------------------------------------------------------------

Canonical = tuple


def canonical_expected(value: Any) -> Canonical:
    """A value as the matrix records it."""
    if value == "null" or value is None:
        return ("null",)
    if not isinstance(value, dict) or len(value) != 1:
        raise OracleError(f"unrecognised matrix value {value!r}")
    (kind, payload), = value.items()
    if kind == "string":
        return ("string", payload)
    if kind in ("integer", "signed_integer", "count", "vertex_id"):
        # A count, a vertex identity and an integer property all reach a client
        # as an integer; the matrix keeps them apart for HydraDB's in-process
        # comparison, which Neo4j has no equivalent of.
        return ("int", int(payload))
    if kind == "bool":
        return ("bool", bool(payload))
    if kind == "float":
        return ("float", float(payload))
    if kind == "list":
        return ("list", tuple(canonical_expected(item) for item in payload))
    if kind == "map":
        return ("map", tuple(sorted((key, canonical_expected(item)) for key, item in payload.items())))
    if kind == "relationship":
        return ("relationship", int(payload["src"]), payload["type"], int(payload["dst"]))
    raise OracleError(f"unrecognised matrix value kind {kind!r}")


def canonical_neo4j(value: Any) -> Canonical:
    """A value as the Neo4j driver returns it."""
    # Imported lazily so `self-test` runs without the driver installed.
    try:
        from neo4j.graph import Node, Path as GraphPath, Relationship
    except ImportError:  # pragma: no cover - exercised only without the driver
        Node = GraphPath = Relationship = ()  # type: ignore[assignment]

    if value is None:
        return ("null",)
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, int):
        return ("int", value)
    if isinstance(value, float):
        return ("float", value)
    if isinstance(value, str):
        return ("string", value)
    if Node and isinstance(value, Node):
        return ("int", node_identity(value))
    if Relationship and isinstance(value, Relationship):
        return (
            "relationship",
            node_identity(value.start_node),
            value.type,
            node_identity(value.end_node),
        )
    if GraphPath and isinstance(value, GraphPath):
        return ("list", tuple(canonical_neo4j(item) for item in value.relationships))
    if isinstance(value, (list, tuple)):
        return ("list", tuple(canonical_neo4j(item) for item in value))
    if isinstance(value, dict):
        return ("map", tuple(sorted((key, canonical_neo4j(item)) for key, item in value.items())))
    raise OracleError(f"Neo4j returned a value this harness cannot compare: {type(value).__name__}")


def node_identity(node: Any) -> int:
    identity = node.get("id")
    if not isinstance(identity, int) or isinstance(identity, bool):
        raise OracleError("Neo4j returned a node without the seeded integer id property")
    return identity


def values_equal(left: Canonical, right: Canonical) -> bool:
    if left[0] != right[0]:
        return False
    if left[0] == "float":
        if math.isnan(left[1]) or math.isnan(right[1]):
            return math.isnan(left[1]) and math.isnan(right[1])
        return math.isclose(left[1], right[1], rel_tol=FLOAT_RELATIVE_TOLERANCE, abs_tol=0.0)
    if left[0] in ("list",):
        return len(left[1]) == len(right[1]) and all(values_equal(a, b) for a, b in zip(left[1], right[1]))
    if left[0] == "map":
        return len(left[1]) == len(right[1]) and all(
            lk == rk and values_equal(lv, rv) for (lk, lv), (rk, rv) in zip(left[1], right[1])
        )
    return left == right


def rows_equal(left: list[tuple[Canonical, ...]], right: list[tuple[Canonical, ...]], positional: bool) -> bool:
    if len(left) != len(right):
        return False
    if not positional:
        left = sorted(left, key=sort_key)
        right = sorted(right, key=sort_key)
    return all(len(a) == len(b) and all(values_equal(x, y) for x, y in zip(a, b)) for a, b in zip(left, right))


def sort_key(row: tuple[Canonical, ...]) -> str:
    # Floats are rounded for ordering only, so two values `values_equal` calls
    # equal land next to each other; equality is still decided exactly above.
    def stable(value: Canonical) -> Any:
        kind = value[0]
        if kind == "float":
            return [kind, round(value[1], 6) if math.isfinite(value[1]) else repr(value[1])]
        if kind == "list":
            return [kind, [stable(item) for item in value[1]]]
        if kind == "map":
            return [kind, [[key, stable(item)] for key, item in value[1]]]
        return list(value)

    return json.dumps([stable(value) for value in row], sort_keys=True)


def parameter_value(value: Any) -> Any:
    """A matrix parameter as a driver argument."""
    canonical = canonical_expected(value)
    return driver_value(canonical)


def driver_value(canonical: Canonical) -> Any:
    kind = canonical[0]
    if kind == "null":
        return None
    if kind in ("string", "int", "bool", "float"):
        return canonical[1]
    if kind == "list":
        return [driver_value(item) for item in canonical[1]]
    if kind == "map":
        return {key: driver_value(item) for key, item in canonical[1]}
    raise OracleError(f"a {kind} cannot be a query parameter")


def display(canonical: Canonical) -> Any:
    """A canonical value in the matrix's own spelling, for reports."""
    kind = canonical[0]
    if kind == "null":
        return "null"
    if kind == "int":
        return {"integer": canonical[1]}
    if kind in ("string", "bool", "float"):
        return {kind: canonical[1]}
    if kind == "list":
        return {"list": [display(item) for item in canonical[1]]}
    if kind == "map":
        return {"map": {key: display(item) for key, item in canonical[1]}}
    if kind == "relationship":
        return {"relationship": {"src": canonical[1], "type": canonical[2], "dst": canonical[3]}}
    return repr(canonical)


# ---------------------------------------------------------------------------
# Matrix
# ---------------------------------------------------------------------------


@dataclass
class Row:
    raw: dict[str, Any]
    cell: str

    @property
    def id(self) -> str:
        return self.raw["id"]

    @property
    def query(self) -> str:
        return self.raw["query"]

    @property
    def engine(self) -> str:
        return self.raw["engine"]

    @property
    def legacy(self) -> str:
        return self.raw["legacy"]

    @property
    def positional(self) -> bool:
        return self.raw["ordering"] in POSITIONAL_ORDERINGS

    @property
    def neo4j_expectation(self) -> dict[str, Any] | None:
        return self.raw.get("neo4j")

    @property
    def expects_neo4j_error(self) -> bool:
        expectation = self.neo4j_expectation
        return bool(expectation) and expectation.get("expect") == "error"

    @property
    def has_recorded_result(self) -> bool:
        # A supported row always has a recorded result, even an empty one. An
        # unsupported row has one only once its target answer is written down,
        # which `neo4j.expect == "rows"` declares; an empty `rows` alone cannot
        # tell "answers nothing" from "nobody has said yet".
        if "supported" in (self.engine, self.legacy):
            return True
        expectation = self.neo4j_expectation
        return bool(expectation) and expectation.get("expect") == "rows"

    def parameters(self) -> dict[str, Any]:
        parameters = {name: parameter_value(value) for name, value in self.raw.get("parameters", {}).items()}
        for name, values in self.raw.get("list_parameters", {}).items():
            parameters[name] = [parameter_value(value) for value in values]
        return parameters

    def expected_rows(self) -> list[tuple[Canonical, ...]]:
        return [tuple(canonical_expected(value) for value in row) for row in self.raw["rows"]]


@dataclass
class Divergence:
    row: str
    reason: str
    reviewer: str
    kind: str


@dataclass
class Fixture:
    default_cell: str
    vertices: dict[str, list[dict[str, Any]]] = field(default_factory=lambda: defaultdict(list))
    edges: dict[str, list[dict[str, Any]]] = field(default_factory=lambda: defaultdict(list))


def load_matrix(path: Path) -> tuple[Fixture, list[Row]]:
    matrix = json.loads(path.read_text())
    if matrix.get("version") != 1:
        raise OracleError(f"{path}: unknown matrix version {matrix.get('version')!r}")
    raw_fixture = matrix["fixture"]
    fixture = Fixture(default_cell=raw_fixture["cell"])
    for vertex in raw_fixture["vertices"]:
        fixture.vertices[vertex.get("cell", fixture.default_cell)].append(vertex)
    for edge in raw_fixture["edges"]:
        fixture.edges[edge.get("cell", fixture.default_cell)].append(edge)
    validate_fixture(fixture)
    rows = [Row(raw=row, cell=row.get("cell") or fixture.default_cell) for row in matrix["rows"]]
    seen: set[str] = set()
    for row in rows:
        if row.id in seen:
            raise OracleError(f"duplicate matrix row id {row.id}")
        seen.add(row.id)
        expectation = row.neo4j_expectation
        if expectation is not None:
            if expectation.get("expect") not in ("error", "rows"):
                raise OracleError(f"row {row.id}: neo4j.expect must be 'error' or 'rows'")
            if expectation.get("expect") == "error" and not expectation.get("error_code"):
                raise OracleError(f"row {row.id}: an expected error needs error_code")
            if not expectation.get("reason"):
                raise OracleError(f"row {row.id}: a neo4j expectation needs a reason")
    return fixture, rows


def validate_fixture(fixture: Fixture) -> None:
    for cell, vertices in fixture.vertices.items():
        ids = [vertex["id"] for vertex in vertices]
        if len(ids) != len(set(ids)):
            raise OracleError(f"cell {cell}: duplicate vertex id")
        for vertex in vertices:
            if "id" in vertex["properties"]:
                # `id` is reserved for the vertex identity the seeder writes;
                # a stored property of that name would shadow it.
                raise OracleError(f"cell {cell}: vertex {vertex['id']} stores an `id` property")
    for cell, edges in fixture.edges.items():
        keys = [(edge["type"], edge["src"], edge["dst"]) for edge in edges]
        if len(keys) != len(set(keys)):
            raise OracleError(
                f"cell {cell}: two edges share (type, src, dst); HydraDB stores one, Neo4j would store both"
            )


def load_divergences(path: Path) -> dict[str, Divergence]:
    if not path.exists():
        return {}
    document = json.loads(path.read_text())
    divergences: dict[str, Divergence] = {}
    for entry in document.get("divergences", []):
        for required in ("row", "reason", "reviewer", "kind"):
            if not entry.get(required):
                raise OracleError(f"{path}: a divergence is missing {required!r}")
        if entry["kind"] not in {"values", "types", "ordering", "columns", "semantics"}:
            raise OracleError(f"{path}: unknown divergence kind {entry["kind"]!r}")
        if entry["row"] in divergences:
            raise OracleError(f"{path}: row {entry['row']} is listed twice")
        divergences[entry["row"]] = Divergence(entry["row"], entry["reason"], entry["reviewer"], entry["kind"])
    return divergences


# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------


@dataclass
class Observation:
    columns: list[str] | None = None
    rows: list[tuple[Canonical, ...]] | None = None
    error: str | None = None
    gql_status: str | None = None
    elapsed_ms: float = 0.0


def verdict(row: Row, observed: Observation, divergence: Divergence | None) -> str:
    """What one row's Neo4j observation means. Pure, so `self-test` covers it."""
    if row.expects_neo4j_error:
        if observed.error is not None:
            expected = row.neo4j_expectation or {}
            code = observed.error.split(": ", 1)[0]
            matches = code == expected.get("error_code")
            if expected.get("gql_status") is not None:
                matches = matches and observed.gql_status == expected["gql_status"]
            return "refused" if matches else "refusal-mismatch"
        return "unexpected-success"

    if not row.has_recorded_result:
        # Nothing is written down for this shape yet. Neo4j's answer is the
        # starting point for recording one; agreement cannot be judged.
        return "gap-unrecorded" if observed.error is None else "gap-neo4j-refuses"

    if observed.error is not None:
        agrees = False
        kind = "unexpected-error"
    elif observed.columns != row.raw["columns"]:
        agrees = False
        kind = "column-mismatch"
    else:
        agrees = rows_equal(observed.rows or [], row.expected_rows(), row.positional)
        kind = "mismatch"

    if divergence is not None:
        if agrees:
            return "stale-divergence"
        if observed.error is not None:
            return "unexpected-error"
        allowed = divergence.kind == "columns" if kind == "column-mismatch" else divergence.kind in {"values", "semantics"}
        if kind == "mismatch" and divergence.kind == "ordering":
            allowed = row.positional and rows_equal(observed.rows or [], row.expected_rows(), positional=False)
        if kind == "mismatch" and divergence.kind == "types":
            def numeric_payload(value: Canonical) -> Canonical:
                if value[0] in {"int", "float"}:
                    return ("number", value[1])
                if value[0] == "list":
                    return ("list", tuple(numeric_payload(v) for v in value[1]))
                return value
            actual = [tuple(numeric_payload(v) for v in record) for record in observed.rows or []]
            expected = [tuple(numeric_payload(v) for v in record) for record in row.expected_rows()]
            allowed = rows_equal(actual, expected, row.positional)
        return "divergence-allowed" if allowed else "divergence-kind-mismatch"
    return "agree" if agrees else kind


# ---------------------------------------------------------------------------
# Neo4j
# ---------------------------------------------------------------------------


def seed_cell(session: Any, fixture: Fixture, cell: str) -> None:
    session.run("MATCH (n) DETACH DELETE n").consume()
    vertices = [
        {
            "id": vertex["id"],
            "label": vertex["label"],
            "properties": {name: parameter_value(value) for name, value in vertex["properties"].items()},
        }
        for vertex in fixture.vertices.get(cell, [])
    ]
    session.run(
        "CYPHER 25 UNWIND $vertices AS v "
        "CREATE (n:$(v.label)) SET n = v.properties, n.id = v.id",
        vertices=vertices,
    ).consume()
    edges = [
        {
            "type": edge["type"],
            "src": edge["src"],
            "dst": edge["dst"],
            "properties": {name: parameter_value(value) for name, value in edge.get("properties", {}).items()},
        }
        for edge in fixture.edges.get(cell, [])
    ]
    summary = session.run(
        "CYPHER 25 UNWIND $edges AS e "
        "MATCH (a {id: e.src}), (b {id: e.dst}) "
        "CREATE (a)-[r:$(e.type)]->(b) SET r = e.properties",
        edges=edges,
    ).consume()
    created = summary.counters.relationships_created
    if created != len(edges):
        raise OracleError(f"cell {cell}: seeded {created} of {len(edges)} edges; an endpoint is missing")


def observe(session: Any, row: Row) -> Observation:
    from neo4j.exceptions import Neo4jError

    started = time.perf_counter()
    try:
        result = session.run(f"CYPHER 25 {row.query}", row.parameters())
        columns = list(result.keys())
        rows = [tuple(canonical_neo4j(value) for value in record.values()) for record in result]
        return Observation(columns=columns, rows=rows, elapsed_ms=(time.perf_counter() - started) * 1000)
    except Neo4jError as error:
        return Observation(
            error=f"{error.code}: {error.message}",
            gql_status=getattr(error, "gql_status", None),
            elapsed_ms=(time.perf_counter() - started) * 1000,
        )


def neo4j_version(session: Any) -> str:
    for record in session.run("CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition"):
        if record["name"] == "Neo4j Kernel":
            return f"Neo4j {record['versions'][0]} {record['edition']}"
    raise OracleError("the server did not report a Neo4j Kernel component")


def run(arguments: argparse.Namespace) -> int:
    from neo4j import GraphDatabase

    password = os.environ.get(arguments.password_env)
    if password is None and arguments.password_file:
        password = Path(arguments.password_file).read_text().strip()
    if not password:
        print(f"set {arguments.password_env} or pass --password-file", file=sys.stderr)
        return 2

    fixture, rows = load_matrix(Path(arguments.matrix))
    divergences = load_divergences(Path(arguments.divergences))
    unknown = set(divergences) - {row.id for row in rows}
    if unknown:
        raise OracleError(f"divergences name rows the matrix does not have: {sorted(unknown)}")
    if arguments.only:
        wanted = set(arguments.only)
        rows = [row for row in rows if row.id in wanted]
        missing = wanted - {row.id for row in rows}
        if missing:
            raise OracleError(f"--only names unknown rows: {sorted(missing)}")

    by_cell: dict[str, list[Row]] = defaultdict(list)
    for row in rows:
        by_cell[row.cell].append(row)

    run_id = arguments.run_id or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    jsonl = open(arguments.jsonl, "w") if arguments.jsonl else None
    counts: dict[str, int] = defaultdict(int)
    failures: list[tuple[Row, str, Observation]] = []
    gaps: list[tuple[Row, Observation]] = []

    driver = GraphDatabase.driver(
        arguments.uri,
        auth=(arguments.user, password),
        notifications_min_severity="OFF",
    )
    try:
        with driver.session(database=arguments.database) as session:
            version = neo4j_version(session)
            print(f"oracle: {version} at {arguments.uri}, {len(rows)} rows in {len(by_cell)} cells")
            for cell in sorted(by_cell):
                seed_cell(session, fixture, cell)
                for row in by_cell[cell]:
                    if "excluded" in (row.engine, row.legacy):
                        # Native path procedures and other reviewed exclusions
                        # do not run as ordinary reads on HydraDB, and their
                        # procedures do not exist on Neo4j.
                        observed = Observation()
                        outcome = "excluded"
                    else:
                        observed = observe(session, row)
                        outcome = verdict(row, observed, divergences.get(row.id))
                    counts[outcome] += 1
                    if outcome in FAILING_VERDICTS:
                        failures.append((row, outcome, observed))
                    elif outcome in ("gap-unrecorded", "gap-neo4j-refuses"):
                        gaps.append((row, observed))
                    if jsonl:
                        jsonl.write(
                            json.dumps(
                                {
                                    "run_id": run_id,
                                    "commit": arguments.commit,
                                    "neo4j": version,
                                    "row_id": row.id,
                                    "cell": row.cell,
                                    "legacy": row.legacy,
                                    "engine": row.engine,
                                    "verdict": outcome,
                                    "elapsed_ms": round(observed.elapsed_ms, 3),
                                    "columns": observed.columns,
                                    "rows": [[display(value) for value in record] for record in observed.rows or []],
                                    "error": observed.error,
                                    "gql_status": observed.gql_status,
                                    "query": row.query,
                                }
                            )
                            + "\n"
                        )
    finally:
        driver.close()
        if jsonl:
            jsonl.close()

    for row, observed in gaps:
        print(f"\nGAP {row.id} [{row.cell}]  {row.query}")
        if observed.error:
            print(f"  neo4j refuses: {observed.error}")
        else:
            print(f"  neo4j columns: {observed.columns}")
            print(f"  neo4j rows:    {json.dumps([[display(v) for v in r] for r in observed.rows or []])}")
    for row, outcome, observed in failures:
        print(f"\nFAIL {outcome} {row.id} [{row.cell}]  {row.query}")
        if observed.error:
            print(f"  neo4j error: {observed.error}")
        else:
            print(f"  neo4j columns: {observed.columns}  recorded: {row.raw['columns']}")
            print(f"  neo4j rows:    {json.dumps([[display(v) for v in r] for r in observed.rows or []])}")
            print(f"  recorded rows: {json.dumps(row.raw['rows'])}")

    summary = ", ".join(f"{name}={counts[name]}" for name in sorted(counts))
    print(f"\nsummary: {summary}")
    if failures or (arguments.strict_gaps and gaps):
        return 1
    return 0


# ---------------------------------------------------------------------------
# Offline self-test
# ---------------------------------------------------------------------------


def self_test(arguments: argparse.Namespace) -> int:
    def check(condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(message)

    # Types stay apart: 1 is not 1.0, true is not 1, a vertex is its id.
    check(not values_equal(("int", 1), ("float", 1.0)), "int and float must differ")
    check(canonical_expected({"bool": True}) != canonical_expected({"integer": 1}), "bool is not int")
    check(canonical_expected({"vertex_id": 7}) == ("int", 7), "a vertex id compares as its integer")
    check(canonical_expected({"count": 3}) == ("int", 3), "a count compares as an integer")
    check(canonical_expected("null") == ("null",), "null spelling")
    check(values_equal(("float", 0.1 + 0.2), ("float", 0.3)), "floats compare within tolerance")
    check(not values_equal(("float", 1.0), ("float", 1.001)), "float tolerance is tight")
    check(values_equal(("float", float("nan")), ("float", float("nan"))), "NaN equals NaN for reporting")
    check(
        values_equal(
            canonical_expected({"list": [{"integer": 1}, "null"]}),
            ("list", (("int", 1), ("null",))),
        ),
        "lists compare element-wise",
    )
    check(canonical_neo4j(True) == ("bool", True), "driver bool stays bool")
    check(canonical_neo4j(2) == ("int", 2), "driver int")
    check(canonical_neo4j([1, None]) == ("list", (("int", 1), ("null",))), "driver list")

    # Ordering contract: positional rows must match in order, multisets need not.
    a = [(("int", 1),), (("int", 2),)]
    b = [(("int", 2),), (("int", 1),)]
    check(rows_equal(a, b, positional=False), "multiset ignores order")
    check(not rows_equal(a, b, positional=True), "positional honours order")
    check(not rows_equal(a, a + a[:1], positional=False), "multiplicity is asserted")

    # Verdicts.
    def make_row(**overrides: Any) -> Row:
        raw = {
            "id": "r",
            "query": "RETURN 1 AS x",
            "legacy": "supported",
            "engine": "supported",
            "columns": ["x"],
            "rows": [[{"integer": 1}]],
            "ordering": "single-row",
        }
        raw.update(overrides)
        return Row(raw=raw, cell="c")

    ok = Observation(columns=["x"], rows=[(("int", 1),)])
    wrong = Observation(columns=["x"], rows=[(("int", 2),)])
    error = Observation(error="Neo.ClientError.Statement.SyntaxError: no")
    allowed = Divergence("r", "reason", "reviewer", "semantics")
    check(verdict(make_row(), ok, None) == "agree", "agree")
    check(verdict(make_row(), wrong, None) == "mismatch", "mismatch")
    check(verdict(make_row(), error, None) == "unexpected-error", "unexpected error")
    check(verdict(make_row(), Observation(columns=["y"], rows=[(("int", 1),)]), None) == "column-mismatch", "columns")
    check(verdict(make_row(), wrong, allowed) == "divergence-allowed", "allowlisted divergence")
    check(verdict(make_row(), ok, allowed) == "stale-divergence", "a divergence that agrees again is stale")
    check(verdict(make_row(), error, allowed) == "unexpected-error", "a value divergence never allows query errors")
    numeric_type = Observation(columns=["x"], rows=[(("float", 1.0),)])
    type_allow = Divergence("r", "reason", "reviewer", "types")
    check(verdict(make_row(), numeric_type, type_allow) == "divergence-allowed", "type allowance permits only equal numeric payload")
    check(verdict(make_row(), wrong, type_allow) == "divergence-kind-mismatch", "type allowance cannot hide changed values")
    order_allow = Divergence("r", "reason", "reviewer", "ordering")
    check(verdict(make_row(), wrong, order_allow) == "divergence-kind-mismatch", "ordering allowance cannot hide changed values")
    only_columns = Divergence("r", "reason", "reviewer", "columns")
    check(verdict(make_row(), wrong, only_columns) == "divergence-kind-mismatch", "column divergence cannot allow wrong values")
    refused = make_row(neo4j={"expect": "error", "reason": "unaliased WITH", "error_code": "Neo.ClientError.Statement.SyntaxError"}, legacy="rejected", engine="unsupported", rows=[], columns=[])
    check(verdict(refused, error, None) == "refused", "expected refusal")
    forbidden = Observation(error="Neo.ClientError.Security.Forbidden: denied")
    check(verdict(refused, forbidden, None) == "refusal-mismatch", "unrelated error cannot confirm refusal")
    check(verdict(refused, ok, None) == "unexpected-success", "refusal that succeeds")
    gap = make_row(legacy="rejected", engine="unsupported", rows=[], columns=[])
    check(verdict(gap, ok, None) == "gap-unrecorded", "unrecorded gap")
    recorded_gap = make_row(legacy="rejected", engine="unsupported", neo4j={"expect": "rows", "reason": "target"})
    check(verdict(recorded_gap, ok, None) == "agree", "a recorded gap is held to its rows")
    check(verdict(recorded_gap, wrong, None) == "mismatch", "a recorded gap can mismatch")

    # The checked-in matrix and allowlist load and keep the seeder's rules.
    fixture, rows = load_matrix(Path(arguments.matrix))
    divergences = load_divergences(Path(arguments.divergences))
    check(rows, "matrix has rows")
    unknown = set(divergences) - {row.id for row in rows}
    check(not unknown, f"divergences name unknown rows: {sorted(unknown)}")
    for row in rows:
        if row.has_recorded_result and not row.expects_neo4j_error:
            row.expected_rows()  # every recorded value has a canonical form
        row.parameters()  # every parameter has a driver form
    print(f"self-test: ok ({len(rows)} rows, {sum(len(v) for v in fixture.vertices.values())} vertices, {len(divergences)} divergences)")
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--matrix", default=str(DEFAULT_MATRIX))
    parser.add_argument("--divergences", default=str(DEFAULT_DIVERGENCES))
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("self-test", help="offline checks; no Neo4j needed")

    live = commands.add_parser("run", help="run the matrix against a live Neo4j")
    live.add_argument("--uri", default=os.environ.get("NEO4J_URI", "bolt://127.0.0.1:7687"))
    live.add_argument("--user", default=os.environ.get("NEO4J_USER", "neo4j"))
    live.add_argument("--password-env", default="NEO4J_PASSWORD", help="environment variable holding the password")
    live.add_argument("--password-file", help="file holding the password, read when the variable is unset")
    live.add_argument("--database", default="neo4j")
    live.add_argument("--jsonl", help="write one JSON line per row here")
    live.add_argument("--run-id")
    live.add_argument("--commit", default=os.environ.get("GIT_COMMIT", ""))
    live.add_argument("--only", nargs="*", help="run only these row ids")
    live.add_argument("--strict-gaps", action="store_true", help="fail on unrecorded gap rows too")

    arguments = parser.parse_args(list(argv) if argv is not None else None)
    try:
        if arguments.command == "self-test":
            return self_test(arguments)
        return run(arguments)
    except OracleError as error:
        print(f"oracle: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
