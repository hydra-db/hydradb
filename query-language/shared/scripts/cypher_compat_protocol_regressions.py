#!/usr/bin/env python3
"""Stack regressions through the experimental node in the local A/B harness.

Uses its existing immutable fixture and both public transports. This script
only reads localhost; it does not create graphs or change a deployment.
"""
import argparse
import json
import os
from pathlib import Path

from cypher_engine_ab_client import BoltClient, HttpClient, canonical_value


def case(name, query, expected=None, parameters=None, refusal=None, columns=None):
    return {"name": name, "query": query, "expected": expected,
            "parameters": parameters or {}, "refusal": refusal, "columns": columns or ["answer"]}


CASES = (
    case("boolean_case", "RETURN CASE (1=1) WHEN true THEN 'yes' ELSE 'no' END AS answer", [["yes"]]),
    case("boolean_case_reverse", "RETURN CASE true WHEN (1=1) THEN 'yes' ELSE 'no' END AS answer", [["yes"]]),
    case("bare_nested_count", "RETURN count(*)+1 AS answer", [[2]]),
    case("numeric_distinct", "UNWIND [1,1.0,2,2.0] AS x RETURN count(DISTINCT x) AS answer", [[2]]),
    case("float_spelling_distinct", "UNWIND [1.0,1.00] AS x RETURN count(DISTINCT x) AS answer", [[1]]),
    case("literal_unwind", "UNWIND [3,1,2] AS x RETURN x AS answer ORDER BY x", [[1], [2], [3]]),
    case("parameter_unwind", "UNWIND $xs AS x RETURN x AS answer ORDER BY x", [[1], [3], [3]], {"xs": [3,1,3]}),
    case("null_unwind", "UNWIND null AS x RETURN x AS answer", []),
    case("scalar_unwind", "UNWIND 5 AS x RETURN x AS answer", [[5]]),
    case("nested_unwind", "UNWIND $xs AS x UNWIND [1] AS y RETURN x AS answer LIMIT 1", [[0]], {"xs": list(range(100))}),
    case("unwind_identifier", "MATCH (unwind:Entity) RETURN count(unwind) AS answer", [[3]]),
    case("future_pattern", "MATCH (a:Entity WHERE a.rank < b.rank), (b:Entity) RETURN count(*) AS answer", [[3]]),
    case("group_hidden_order", "UNWIND [2,2,1] AS x RETURN count(*) AS answer GROUP BY x ORDER BY x", [[1], [2]]),
    case("group_with_hidden_order", "UNWIND [2,2,1] AS x WITH count(*) AS answer GROUP BY x ORDER BY x RETURN answer", [[1], [2]]),
    case("group_empty_constant", "MATCH (n:CompatMissing) RETURN count(*) AS answer GROUP BY 42", []),
    case("group_alias_collision", "UNWIND [1,3] AS x RETURN x % 2 AS __hydradb_group_0 GROUP BY x", [[1], [1]], columns=["__hydradb_group_0"]),
    case("escaped_when", "UNWIND [1] AS `WHEN` RETURN CASE `WHEN` WHEN 1 THEN 'yes' ELSE 'no' END AS answer", [["yes"]]),
    case("nested_in_aggregate", "MATCH (i:Entity) WITH count(*) IN [3] AS answer RETURN answer", [[True]]),
    case("collected_list_coalesce", "MATCH (e:Entity) RETURN size(coalesce(collect(e.entity_id), [])) AS answer", [[3]]),
    case("relationship_unwind", "MATCH (a:Entity {entity_id:'alpha'})-[rs:KNOWS*1..1]->(b) UNWIND rs AS r RETURN count(r) AS answer", [[1]]),
    case("relationship_unwind_properties", "MATCH (a:Event)-[rs:NEXT*1..1]->(b) WITH rs AS edges UNWIND edges AS r RETURN r.weight AS answer ORDER BY answer", [[1.5], [3], [3], [7]]),
    case("conflicting_path", "MATCH (a:Entity {entity_id:'alpha'})-[rs:KNOWS*1..1]->(b) MATCH (c:Entity {entity_id:'beta'})-[rs:KNOWS*1..1]->(d) RETURN count(*) AS answer", [[0]]),
    case("matching_path", "MATCH (a:Entity {entity_id:'alpha'})-[rs:KNOWS*1..1]->(b) MATCH (c:Entity {entity_id:'alpha'})-[rs:KNOWS*1..1]->(d) RETURN count(*) AS answer", [[1]]),
    case("mixed_hop_uniqueness", "MATCH (a:Entity {entity_id:'alpha'})-[rs:KNOWS*1..1]-(b)-[r:KNOWS]-(c) RETURN count(*) AS answer", [[0]]),
    case("separate_clause_reuse", "MATCH (a:Entity {entity_id:'alpha'})-[rs:KNOWS*1..1]-(b) MATCH (b)-[r:KNOWS]-(c) RETURN count(*) AS answer", [[1]]),
    case("numeric_return_distinct", "UNWIND [1,1.0,2] AS x RETURN DISTINCT x AS answer ORDER BY answer", [[1], [2]]),
    case("numeric_union", "RETURN 1 AS answer UNION RETURN 1.0 AS answer", [[1]]),
    case("numeric_group", "UNWIND [1,1.0,2] AS x RETURN count(*) AS answer GROUP BY x ORDER BY x", [[2], [1]]),
    case("nested_numeric_distinct", "UNWIND [[1],[1.0],[2]] AS x RETURN DISTINCT x AS answer ORDER BY answer", [[[1]], [[2]]]),
    case("list_ordering", "RETURN [1] < [2] AS answer", [[True]]),
    case("list_null_ordering", "RETURN [1,null] < [1,2] AS answer", [[None]]),
    case("optional_existing_path", "MATCH (a:Entity {entity_id:'alpha'})-[rs:KNOWS*1..1]->(b) OPTIONAL MATCH (a)-[rs:NEXT*1..1]->(c) WITH rs RETURN size(rs) AS answer", [[1]]),
    case("computed_relationship_lists", "MATCH (a:Event)-[rs:NEXT*1..1]->(b) WITH [rs] AS paths UNWIND paths AS edges WITH edges AS kept UNWIND kept AS r RETURN r.weight AS answer ORDER BY answer", [[1.5], [3], [3], [7]]),
    case("collected_relationship_lists", "MATCH (a:Event)-[r:NEXT]->(b) WITH collect(r) AS edges UNWIND edges AS r RETURN r.weight AS answer ORDER BY answer", [[1.5], [3], [3], [7]]),
    case("empty_star", "RETURN *, 1 AS answer", refusal="no variables in scope"),
    case("optional_undeclared", "OPTIONAL MATCH (a:CompatMissing) RETURN b.name AS answer", refusal="out-of-scope binding b"),
    case("integer_overflow", "RETURN toInteger('9223372036854775808') AS answer", refusal="out of range"),
)


def self_test():
    assert len({c["name"] for c in CASES}) == len(CASES)
    for c in CASES:
        assert c["refusal"] is not None or isinstance(c["expected"], list)
        for row in c["expected"] or []:
            assert len(row) == len(c["columns"])
            for value in row:
                canonical_value(value)
    print(f"protocol regression schema: {len(CASES)} cases")


def run(token):
    results = []
    for transport, client in (("bolt", BoltClient("bolt://127.0.0.1:17687", token)),
                              ("http", HttpClient("http://127.0.0.1:18443", token))):
        with client:
            for c in CASES:
                result = client.request(c["query"], c["parameters"])
                if c["refusal"]:
                    error = result["error"] or {}
                    assert error.get("code") in {"Neo.ClientError.Statement.InvalidSyntax", "invalid_request"}, (c["name"], result)
                    assert c["refusal"] in error.get("message", ""), (c["name"], result)
                else:
                    expected = [[canonical_value(value) for value in row] for row in c["expected"]]
                    assert result["error"] is None, (c["name"], result)
                    assert result["columns"] == c["columns"], (c["name"], result)
                    assert result["rows"] == expected, (c["name"], result, expected)
                    if transport == "http":
                        assert result["read_epoch"] is not None, (c["name"], result)
                results.append({"case": c["name"], "transport": transport, **result})
    return {"status": "pass", "cases": len(CASES), "requests": len(results), "results": results}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.self_test:
        self_test()
    else:
        if args.output is None:
            parser.error("--output is required")
        token = os.environ.get("GRAPH_CYPHER_ENGINE_AB_TOKEN")
        if not token:
            parser.error("GRAPH_CYPHER_ENGINE_AB_TOKEN is required")
        document = run(token)
        args.output.write_text(json.dumps(document, indent=2) + "\n")
        print(f"protocol regressions: {document['requests']} requests passed")
