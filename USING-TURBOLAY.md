# Using Turbolay

Instructions for building an application against a **running** Turbolay
instance. Hand this file to a coding agent and it should be able to connect,
model data, and query without further explanation.

This is not the contributor guide. Nothing here requires the source tree — if
you also have the Turbolay repository and need to build or start the database
yourself, its `README.md` and `AGENTS.md` cover that. If this file reached you
on its own, everything you need is below.

Three values parameterise every example: the address, the bearer token, and the
`cell_id`. The literals used throughout — `127.0.0.1` with ports 7687/8443/9090,
token `local-development-token-32-bytes`, graph `default`, `cell-0` — are the
local-development defaults and work as-is against a locally started instance.
Against a deployed instance, get the real values from whoever runs the server;
nothing lets you discover them from the API.

Every behaviour below was verified against a running server. Where Turbolay
differs from Neo4j — and it differs in ways that will surprise you — the
difference is called out explicitly.

## What Turbolay is, in one paragraph

A graph database: you store **nodes** (vertices) and **typed, directed
relationships** between them, and query with **OpenCypher**. It speaks the
**Bolt** protocol, so official Neo4j drivers work unmodified, and it also has an
HTTP query API for anything that can send JSON. Treat it as "Neo4j-shaped," but
read [Identity and idempotency](#identity-and-idempotency) and
[The dialect](#the-dialect-what-is-rejected) before writing anything. Those two
sections are where the time goes.

## Connecting

A local development instance listens on the addresses below. A deployed one
will differ — ask the operator.

| Purpose | Address | Endpoints |
|---|---|---|
| Bolt (Neo4j drivers) | `127.0.0.1:7687` | — |
| HTTP query API | `127.0.0.1:8443` | `POST /v1/graphs/{graph_id}/query`, `POST /v1/graphs/{graph_id}/queries/{query_id}/cancel`, `GET /healthz` |
| Admin | `127.0.0.1:9090` | `GET /readyz`, `GET /metrics` |

**The two health endpoints are on different ports and are not
interchangeable.** `GET /healthz` lives on the query port (8443) and answers
`{"status":"ok"}`. `GET /readyz` and `GET /metrics` live on the admin port
(9090). Asking 9090 for `/healthz` returns **404**.

`GET /readyz` returns **200 with an empty body** — check the status code, not
the payload, or a silent failure looks identical to readiness.

Authentication is a **bearer token**, supplied by whoever runs the server. Over
Bolt it goes in the password field, with any username (`neo4j` by convention).

Deployed instances require TLS: use `neo4j+s://` for a publicly trusted
certificate, `neo4j+ssc://` for a self-signed one, and `https://` for the query
API. Plaintext `bolt://` and `http://` are local development only.

`cell_id` selects a partition within a graph. There is **no API to enumerate
cells** — the operator tells you which exist. A wrong `cell_id` fails with an
opaque HTTP 500 (`{"error":{"code":"internal",...}}`), not a helpful message, so
check it first when a query fails inexplicably.

### Bolt, Python

Requires the driver (`pip install neo4j` — use a virtualenv on Homebrew or
Debian Python, which refuse a bare install under PEP 668). If you cannot install
anything, the HTTP API below needs only the standard library.

```python
from neo4j import GraphDatabase

TOKEN = "local-development-token-32-bytes"
driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=("neo4j", TOKEN))
driver.verify_connectivity()

with driver.session(database="default") as session:
    session.run("MERGE (a {id: 1})-[:FOLLOWS]->(b {id: 2})").consume()
    row = session.run(
        "MATCH (a {id: $src})-[:FOLLOWS]->(b) RETURN b.id AS id", src=1
    ).single(strict=True)
    print(row["id"])          # 2
```

`database="default"` is the graph id. Prefer `neo4j://` (routing) over `bolt://`
against a cluster; a direct `bolt://` address pins you to one node.

### Bolt, JavaScript

Requires the driver: `npm install neo4j-driver`.

```javascript
import neo4j from 'neo4j-driver'

const driver = neo4j.driver(
  'bolt://127.0.0.1:7687',
  neo4j.auth.basic('neo4j', 'local-development-token-32-bytes'),
)
const session = driver.session({ database: 'default' })
const result = await session.run(
  'MATCH (a {id: $src})-[:FOLLOWS]->(b) RETURN b.id AS id', { src: 1 },
)
console.log(result.records.map((r) => r.get('id')))
await session.close(); await driver.close()
```

### HTTP query API

```bash
TOKEN=local-development-token-32-bytes

curl -sS -X POST http://127.0.0.1:8443/v1/graphs/default/query \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{
    "cell_id": "cell-0",
    "query": "MATCH (a {id: $src})-[:FOLLOWS]->(b) RETURN b.id AS id",
    "parameters": {"src": 1}
  }'
```

Send `Accept: application/x-ndjson` to stream rows instead of buffering them.
An `X-Graph-Namespace` header is accepted and required in some deployments; when
the server is configured with a single namespace it is optional.

## Verify the connection before building anything

Use ids you are willing to throw away, and clean up afterwards — a reachable
port is not proof the database works, but neither is polluting your real id
range with test data.

```bash
TOKEN=local-development-token-32-bytes
Q=http://127.0.0.1:8443/v1/graphs/default/query
run() { curl -sS -X POST $Q -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' --data "{\"cell_id\":\"cell-0\",\"query\":\"$1\"}"; }

run 'MERGE (a {id: 999999})-[:SMOKE]->(b {id: 999998})'
run 'MATCH (a {id: 999999})-[:SMOKE]->(b) RETURN b.id AS id'
run 'MATCH (a {id: 999999}) DETACH DELETE a'
run 'MATCH (a {id: 999998}) DETACH DELETE a'
```

The second call must return one row containing
`{"type":"vertex_id","value":999998}`.

## Identity and idempotency

**This is the section that catches people.** Turbolay does not behave like
Neo4j here.

### An id-anchored MATCH does not prove the node exists

`MATCH (n {id: 12345}) RETURN n.id AS id` returns **one row** even when node
`12345` was never created, and `count(*)` on it returns **1**. The same happens
immediately after `DETACH DELETE`. Properties come back `null`, but the row is
there.

```text
MATCH (n {id: 777001}) RETURN n.id AS id     -> [{"type":"vertex_id","value":777001}]   (never created)
MATCH (n:Person {id: 777001}) RETURN n.id    -> []                                       (correct)
```

**This affects node-only matches only.** As soon as the pattern contains a
relationship, an absent id yields no rows, correctly:

```text
MATCH (a {id: 995001}) RETURN a.id AS id            -> 1 phantom row
MATCH (a {id: 995001})-[:F]->(b) RETURN b.id AS id  -> []            (correct)
```

So `MATCH (a {id: 1})-[:FOLLOWS]->(b)` — the shape used throughout this document
— is safe. **Anchor on a label whenever the pattern is a bare node**: existence
checks, "is my id range clean?" checks, and node counts. Written the obvious
way, those report that everything exists.

**An unlabelled node is therefore invisible to every existence check.** A node
created by the edge-batch shape (which cannot write labels) responds to
`MATCH (n {id: X})` with exactly the same phantom row as an id that was never
created — byte-identical, verified — and a label-anchored match cannot see it
either. If a bulk load dies between the edge batch and the label batch, the
resulting nodes are undetectable by query.

The escape hatch is that **deletes work on ids regardless of labels**:

```cypher
UNWIND $rows AS row MATCH (n {id: row.id}) DETACH DELETE n
```

Clear the id range you own with that before seeding, rather than trying to
detect what is there.

**`id` is an upsert key, not an ordinary property.** Writing
`CREATE (a {id: 1})-[:FOLLOWS]->(b {id: 2})` and then
`CREATE (a {id: 1})-[:FOLLOWS]->(b {id: 3})` produces **one** node `1` with two
outgoing edges — not two separate nodes that happen to share a property. Node
identity is the `id` you supply. There is no auto-generated node id to fetch.

**Nodes deduplicate; relationships do not.** Running the *same*
`CREATE (a {id: 1})-[:FOLLOWS]->(b {id: 2})` three times leaves one node `1`,
one node `2`, and **three parallel edges** between them. Every `count(*)` you
compute afterwards is silently inflated. This is the single easiest way to get
wrong numbers.

Choose a write form deliberately:

| Form | Re-running it | Use for |
|---|---|---|
| `CREATE (a {..})-[:R]->(b {..})` | **duplicates the edge every time** | One-shot writes you know run once |
| `MERGE (a {..})-[:R]->(b {..})` | idempotent — 3 runs, 1 edge | Anything that might re-run |
| `UNWIND $rows AS row CREATE ...` | idempotent — 3 runs, 1 edge | Bulk seeding |

Verified: 3× `MERGE` → 1 edge. 3× `UNWIND … CREATE` → 1 edge. 3× plain
`CREATE` → 3 edges.

Yes, that means `CREATE` behaves differently inside `UNWIND` than alone — the
batch path is a distinct upsert-style write, not a loop over single `CREATE`s.
Both rows of the table above are correct; rely on the table, not on intuition
carried over from either form. In particular, "relationships do not deduplicate"
applies to **single-statement `CREATE` only**; the batch form deduplicates on
(src, dst, type) and is safe to re-run.

**Prefer `MERGE` or the `UNWIND` batch form for any script that might run
twice.** If you must use plain `CREATE`, make your seed script delete first.

The practical defence is to **verify your id range is empty before seeding**, or
have the script clear the ids it owns. A single stale edge left by an earlier
run silently inflates every count you compute afterwards, and you cannot fall
back on `count(DISTINCT …)` — that is rejected. `DETACH DELETE` on an id that
does not exist is a harmless no-op returning `[]`.

## Response shape

Success:

```json
{
  "query_id": "http-query-2",
  "columns": ["id"],
  "rows": [[{"type": "vertex_id", "value": 2}]],
  "read_epoch": 1,
  "next_cursor": null,
  "bookmark": "sgk:1:64656661756c74:64656661756c74:63656c6c2d30:1"
}
```

Cells are **typed**: `{"type": ..., "value": ...}`. Types include `vertex_id`,
`integer`, `float`, `string`, `boolean`, `list`, `path`, and `null`.

Note that reading a node's `id` yields **`vertex_id`, not `integer`** — a
decoder keyed on `type == "integer"` silently skips every id. Aggregates go the
other way: `count(*)` returns `integer`, even when counting nodes.

**A null cell is `{"type": "null"}` with no `value` key at all.** Reading
`cell["value"]` unconditionally raises `KeyError` the first time a property is
absent — which `OPTIONAL MATCH` produces routinely. Always:

```python
value = cell.get("value")        # None when cell["type"] == "null"
```

Nesting is not consistent, so check per type. A `list` cell contains **typed
cells**:

```json
{"type": "list", "value": [{"type": "vertex_id", "value": 2002},
                           {"type": "vertex_id", "value": 2003}]}
```

A `path` cell uses a **third encoding of its own** (partly plain, partly tagged
with capitalised names) — see [Path queries](#path-queries).

A mutation returns `"columns": []`, `"rows": []`, `"read_epoch": null`. That is
success, not an empty result.

`read_epoch` is the storage sequence the read was pinned to. It grows with
database activity — a few thousand on a lightly used instance — and the `1` in
the example above is simply an early value. Use `bookmark` for consistency
reasoning; `read_epoch` is diagnostic only.

Errors use a different envelope:

```json
{"error": {"code": "invalid_request", "message": "OpenCypher parse error: Invalid input 'R': expected ')'"}}
```

| Status | `code` | Meaning |
|---|---|---|
| 400 | `invalid_request` | Parse error, unsupported construct, missing parameter |
| 401 | `unauthenticated` | Missing or wrong bearer token |
| 422 | *(none)* | The body could not be deserialized at all — see below |
| 4xx | `resource_exhausted` | An admission-control limit was exceeded — batch items (1024) or `page_size` (4096). Both name their limit in the message |
| 500 | `internal` | Server-side failure — an unknown `cell_id` surfaces here |

**A third envelope exists.** If the request body is malformed — most commonly a
missing `cell_id`, which is **mandatory on every request** — the server answers
**422** with a bare framework string and *neither* `rows` nor `error`:

```text
Failed to deserialize the JSON body into the target type: missing field `cell_id` at line 1 column 49
```

The `at line 1 column N` suffix varies with the request body; match on the
prefix, as with every other error.

A client that branches only on `rows` versus `error` reads this as zero rows.
Guard explicitly:

```python
if "error" in payload:  raise ...
if "rows" not in payload:  raise ...   # 422, or anything unexpected
```

## The dialect: what is rejected

Valid Neo4j that Turbolay rejects. The messages below are the distinctive part
of what the server returns; each arrives **wrapped** as
`OpenCypher query is not supported yet: <text> in Query engine`. Match on a
substring, never on the whole string.

| You wrote | Rejected with | Write instead |
|---|---|---|
| `RETURN *` | `RETURN * is not executable in Query engine` | Name every column: `RETURN b.id AS id` |
| `(a)-[:FOLLOWS]-(b)` | `undirected relationships are not executable` | Pick a direction: `-[:FOLLOWS]->` |
| `[:FOLLOWS*]`, `[:FOLLOWS*2..]` | `unbounded variable-length MATCH requires an explicit …` | Bound it: `[:FOLLOWS*1..3]` |
| `(a)-->(b)` | `relationship pattern must have exactly one type` | Name the type: `-[:FOLLOWS]->` |
| `MATCH (a) RETURN a.id` | `node-only MATCH requires an id, label, or property predicate` | Anchor it: `MATCH (a:Person)` or `MATCH (a {id: 1})` |
| `WITH b.id AS x` | `WITH must pass through every in-scope binding` | `WITH` cannot reshape; project in `RETURN` |
| `MERGE (n {id: 1}) SET n.name = 'x'` | `MERGE with following clauses is not executable` | Two statements: `MERGE`, then `MATCH … SET` |
| `CREATE (n:Person {id: 1})` (no edge) | `only one-hop edge patterns are …` | Create the node as an endpoint of a relationship |
| `MERGE (n:Person {id: 1})` (no edge) | `only one-hop edge patterns are …` | Same — writes need an edge pattern |
| `RETURN p` (any path binding) | `RETURN currently supports <binding>.<property> or count(*)` | Return properties, or use `CALL algo.SPpaths … YIELD path` |
| `RETURN b.id + 100` | same message | No arithmetic or expressions in `RETURN`; compute client-side |
| `UNWIND [7,8] AS n` | `UNWIND batch input must be a parameter` | Pass a parameter — see below |
| `UNWIND $rows AS row MERGE (a)-[:R]->(b)` | `UNWIND vertex upsert requires MERGE by id` | Use `CREATE` in the batch form |
| `UNWIND $rows AS row MERGE (n {id: row.id}) SET n.name = row.name` | `UNWIND vertex upsert requires exactly one SET label` | Add the label: `SET n:Person, n.name = row.name` |
| `UNWIND $rows AS row CREATE (a:Person {..})-[:R]->(b:Person {..})` | `UNWIND batch node patterns do not …` | Labels cannot go in the batch *pattern* — see the two batch shapes below |
| `count(DISTINCT b.id)` | `DISTINCT aggregate arguments are not executable` | `DISTINCT` works as a row projection, not inside an aggregate |

**Single-statement writes accept only one-hop edge patterns.**
`CREATE (n:Person {id: 1})` and `MERGE (n:Person {id: 1})` are both rejected with
`only one-hop edge patterns are …`, so an ordinary write cannot create an
edgeless node.

**There are two distinct batch shapes, and they do different things:**

| Shape | What it does | Labels | Properties |
|---|---|---|---|
| `UNWIND $rows AS row CREATE (a {id: row.src})-[:R]->(b {id: row.dst})` | creates edges and their endpoint nodes | **no** — a label in the pattern is rejected | no |
| `UNWIND $rows AS row MERGE (n {id: row.id}) SET n:Person, n.name = row.name` | creates edgeless nodes **and labels/updates existing ones** | exactly **one**, via `SET` | **yes** — after the label |

Two consequences of the second shape, and the second is easy to miss:

- It is the only way to create an **edgeless** node, and such a node can carry
  one label and nothing else.
- Run it over ids that **already exist** and it attaches the label to all of
  them in a single request, leaving their edges untouched. That is what makes
  the two-request bulk recipe possible — see
  [Batch writes](#batch-writes).

**Properties batch too, as long as the label assignment comes first.** Exactly
one `SET n:Label` is required; property assignments may follow it in the same
`SET`:

```cypher
UNWIND $rows AS row MERGE (n {id: row.id})
SET n:Person, n.name = row.name, n.city = row.city
```

Dropping the label half is what fails (`UNWIND vertex upsert requires exactly
one SET label`) — not the properties. So a bulk load is **two requests**: one
edge batch, one node batch carrying labels and properties together.

`shortestPath(...)` itself **parses and executes**. What fails is returning the
path binding — and that fails identically for any path binding, shortest or not.
Use `algo.SPpaths` when you need the path itself; `shortestPath` is fine if you
only return properties of its endpoints.

**`RETURN` is narrow.** It accepts `<binding>.<property>`, aliases, and
aggregates (`count`, `sum`, `avg`, `collect`) — no arithmetic, no expressions, no
bare bindings. The server's own error message says "or `count(*)`", which
understates it: the other aggregates do work.

**A relationship pattern must name exactly one type**, `-[r]->` and
`-[:A|B]->` are both rejected, and nothing enumerates the types present
(`CALL db.relationshipTypes()` fails). So "count every relationship in the
graph" is only possible if your application already knows which types it wrote —
scan once per known type and sum.

There is **no unanchored scan** either — every `MATCH` needs an id, label, or
property predicate. But a label anchor is enough for graph-wide queries, in one request:

```cypher
MATCH (a:Person)-[:FOLLOWS]->(b) RETURN count(*) AS total
```

**This count is only complete if every source node carries the label.** The
anchor constrains the *source* of the edge, so an edge whose source was never
labelled is invisible to it — and the result is silently too low, with no error.
Verified: two edges created, one source labelled, `count(*)` returned **1**.

That is exactly the state the two-request bulk recipe passes through — edges
first, labels second. **If the label batch fails or is interrupted, every
graph-wide count is permanently wrong**, and the orphaned nodes cannot be found
by any query (see
[An id-anchored MATCH does not prove the node exists](#an-id-anchored-match-does-not-prove-the-node-exists)).

So: label your nodes when you create them, treat the label batch as part of the
write rather than a follow-up, and reconcile the count against the number of
edges you believe you wrote before trusting it.

An undefined parameter fails fast with `missing OpenCypher query parameter
$missing` rather than binding null.

### What is supported

Verified against a running instance:

- `MATCH` over node patterns and typed, directed relationships; multi-edge paths
  in one `MATCH`; **comma-joined patterns** (`MATCH (a)-[:R]->(b), (c)-[:R]->(b)`)
- `OPTIONAL MATCH`
- Bounded variable-length: `[:FOLLOWS*1..3]`
- Node labels, node and relationship properties, id constraints
- `WHERE` with `=`, `<>`, `<`, `>`, `<=`, `>=` and boolean combinations
- `ORDER BY`, `SKIP`, `LIMIT`, aliases, parameters, `DISTINCT`
- Aggregates: `count(*)`, `count(expr)`, `sum`, `avg`, `collect`
- **Grouped aggregation** — non-aggregated columns act as an implicit
  `GROUP BY`, and **more than one is allowed**:
  `RETURN a.id AS id, a.name AS name, count(*) AS n ORDER BY n DESC LIMIT 10`
  returns id, name and degree together in one request.
  **It emits no row for a node with no matches**, so a person who follows nobody
  is absent from the result rather than present with `0`. Either query each node
  individually (a single-node `count(*)` does return `0`), or default the missing
  ids to `0` client-side.
- `ORDER BY` on an aggregate's alias — `RETURN a.id AS id, count(*) AS n
  ORDER BY n DESC LIMIT 10` (the top-N-by-degree shape) works, and **multiple
  sort keys are accepted**: `ORDER BY n DESC, id ASC`. Use a secondary key
  whenever a top-N can tie, or the winners are non-deterministic
- `UNION` and `UNION ALL`
- Mutations: `CREATE`, `MERGE`, `DELETE`, `DETACH DELETE`, `SET`, `REMOVE`
- `WITH`, only as a pass-through preserving every binding

### Variable-length traversals are not simple

`[:FOLLOWS*a..b]` **revisits nodes**. In a cycle A → B → C → A, asking for
`*3..3` from A returns **A**:

```text
MATCH (a {id: A})-[:F*3..3]->(b) RETURN b.id   ->  [A]
```

So "who can X reach in N hops" reports X as reachable from itself whenever a
cycle of length N passes through it. Filter the start node out client-side, or
add `WHERE b.id <> $start`. (The `algo.*` path procedures *are* simple — no
vertex is revisited — which is the opposite behaviour.)

### Projecting a source property fans out

`MATCH (n:Person {id: X})-[:FOLLOWS]->(b) RETURN n.name AS name` returns **one
row per edge**, not one row per node — 45 identical rows for a node with 45
out-edges — and **no rows at all** if it has none. To read a property of a
single node, match the bare node and anchor on a label:

```cypher
MATCH (n:Person {id: 1}) RETURN n.name AS name
```

### Hop counts are inclusive

`[:FOLLOWS*1..2]` returns one-hop **and** two-hop neighbours. For *exactly* two
hops use `[:FOLLOWS*2..2]`. Verified: from a node with a 1-hop and a 2-hop
neighbour, `*1..2` returned both and `*2..2` returned only the far one.

## Worked examples

Seed with `MERGE` so re-running is safe:

```cypher
MERGE (a:Person {id: 1, name: 'alice'})-[:FOLLOWS]->(b:Person {id: 2, name: 'bob'})
MERGE (a:Person {id: 1})-[:FOLLOWS]->(b:Person {id: 3, name: 'carol'})
MERGE (a:Person {id: 2})-[:FOLLOWS]->(b:Person {id: 5, name: 'erin'})
```

```cypher
-- direct neighbours
MATCH (a {id: 1})-[:FOLLOWS]->(b) RETURN b.id AS id ORDER BY id

-- one and two hops
MATCH (a {id: 1})-[:FOLLOWS*1..2]->(b) RETURN b.id AS id ORDER BY id

-- exactly two hops
MATCH (a {id: 1})-[:FOLLOWS*2..2]->(b) RETURN b.id AS id ORDER BY id

-- filter and count
MATCH (a {id: 1})-[:FOLLOWS]->(b) WHERE b.id > 2 RETURN count(*) AS n

-- out-degree of one person. Returns 0 correctly when they follow nobody.
MATCH (a:Person {id: 1})-[:FOLLOWS]->(b) RETURN count(*) AS n

-- distinct set (count(DISTINCT ...) is rejected, so count client-side)
MATCH (a {id: 1})-[:FOLLOWS*1..2]->(b) RETURN DISTINCT b.id AS id ORDER BY id

-- aggregate into a list
MATCH (a {id: 1})-[:FOLLOWS]->(b) RETURN collect(b.id) AS ids

-- common neighbours: two patterns joined by a comma, sharing binding b
MATCH (x {id: 1})-[:FOLLOWS]->(b), (y {id: 2})-[:FOLLOWS]->(b)
RETURN b.id AS id ORDER BY id

-- OPTIONAL MATCH on a node with no outgoing edges yields one row of NULL,
-- not zero rows -- so count(*) here returns 1, not 0. Count a property instead.
MATCH (a:Person {id: 5}) OPTIONAL MATCH (a)-[:FOLLOWS]->(b) RETURN b.id AS id
MATCH (a:Person {id: 5}) OPTIONAL MATCH (a)-[:FOLLOWS]->(b) RETURN count(b.id) AS n

-- combine two shapes
MATCH (a {id: 1})-[:FOLLOWS]->(b) RETURN b.id AS id
UNION ALL
MATCH (a {id: 2})-[:FOLLOWS]->(c) RETURN c.id AS id
```

**`count(*)` counts rows, so the way you wrote the match decides the answer for
an empty result.** All three of these are consistent once you see that:

| Query | Rows matched | `count(*)` |
|---|---|---|
| `MATCH (a:Person {id: 5})-[:FOLLOWS]->(b)` | none | **0** — aggregate over an empty set |
| `MATCH (a:Person {id: 5}) OPTIONAL MATCH (a)-[:FOLLOWS]->(b)` | one null row | **1** — use `count(b.id)` for **0** |
| `MATCH (a:Person)-[:FOLLOWS]->(b) RETURN a.id, count(*)` (grouped) | no group emitted | **absent from the result entirely** |

Mutations:

```cypher
MATCH (a {id: 1})-[r:FOLLOWS]->(b {id: 3}) DELETE r
MATCH (a {id: 2}) SET a.name = 'beta'
MATCH (a {id: 21}) DETACH DELETE a
```

**Parameters work in write property maps**, which is the shape most application
code needs:

```json
{"query": "MERGE (a:Person {id: $src, name: $sn})-[:FOLLOWS]->(b:Person {id: $dst, name: $dn})",
 "parameters": {"src": 1, "sn": "alice", "dst": 2, "dn": "bob"}}
```

Repeating the same properties on every mention of a node is safe: `MERGE`
deduplicates on `id` alone, so writing `{id: 1, name: 'alice'}` in ten different
statements still yields one node. You do not need to carry properties only on a
node's first appearance.

Set node and relationship properties inline in the write — that is the cheapest
way, and it works with both `CREATE` and `MERGE`:

```cypher
MERGE (a:Person {id: 1, name: 'alice'})-[:FOLLOWS {since: 2020}]->(b:Person {id: 2, name: 'bob'})
```

To change a property later, `MERGE` cannot be followed by another clause, so use
a separate `MATCH … SET`:

```cypher
MATCH (n:Person {id: 2}) SET n.nickname = 'bobby'
```

### Batch writes

Batch input is always a **parameter** whose elements are maps, read via field
access on the loop variable. The **edge batch** uses `CREATE` (a `MERGE` edge
pattern under `UNWIND` is rejected); the **node batch** below uses `MERGE`. The
edge batch is idempotent — re-running does not duplicate edges.

```json
{
  "cell_id": "cell-0",
  "query": "UNWIND $rows AS row CREATE (a {id: row.src})-[:FOLLOWS]->(b {id: row.dst})",
  "parameters": {"rows": [{"src": 1, "dst": 30}, {"src": 1, "dst": 31}]}
}
```

Use this for bulk ingestion rather than one request per edge. Within and across
batches it deduplicates on **(src, dst, relationship type)** — the same pair
twice in one batch still creates one edge, but the same pair with two different
types correctly creates one edge of each. Loading several relationship types
between the same nodes is safe.

**A batch is capped at 1024 items.** Beyond that, admission control rejects the
whole request:

```text
client_query_batch_items rejected by admission control: actual 5000 exceeds limit 1024
```

The message states the limit, so chunk to 1000 and you are safely inside it.
The error code is `resource_exhausted`. `page_size` is governed the same way
(`client_query_page_size … exceeds limit 4096`); the other budgets in
[Limits that look like bugs](#limits-that-look-like-bugs) do not name numbers.

**Batched `DETACH DELETE` obeys the same 1024 cap** — chunk id-range cleanups
too.

Two more batch shapes exist, both rigidly shaped:

```cypher
-- batched delete: clears up to 1024 ids per request (the batch cap)
UNWIND $rows AS row MATCH (n {id: row.id}) DETACH DELETE n

-- batched read: exactly these two projections and no others --
-- first must be the source field, second must be <binding>.id
UNWIND $rows AS row MATCH (a {id: row.id})-[:FOLLOWS]->(b)
RETURN row.id AS src, b.id AS dst
```

Any other projection list on the batched read is rejected. **There is no batch
shape that writes properties**: `UNWIND … MATCH … SET` is rejected with
`UNWIND MATCH must end in RETURN or DELETE` — that shape reads and deletes, it
does not write. Use the `MERGE … SET n:Label, n.prop = …` shape above to batch
property writes.

**The edge-batch shape cannot write labels or node properties** — a label in
its pattern is rejected (`UNWIND batch node patterns do not …`). Nodes it
creates therefore have no label, and every `MATCH` needs an id, label, or
property anchor. The fix is a second batch:

```cypher
-- 1. labels AND properties for every node id you are about to connect
UNWIND $rows AS row MERGE (n {id: row.id}) SET n:Person, n.name = row.name

-- 2. the edges
UNWIND $rows AS row CREATE (a {id: row.src})-[:FOLLOWS]->(b {id: row.dst})
```

**Write the labels first.** Either order produces the same end state, but
edges-first passes through a window in which the nodes exist *unlabelled* — and
an unlabelled node is invisible to every existence check and missing from every
graph-wide count (see above). Crash in that window and the damage is permanent
and undetectable. Labels-first has no such window: at every instant the graph is
either missing nodes entirely or fully labelled.

This is safe because **the edge batch does not clobber labels or properties on
ids that already exist** — verified: a node labelled `:Person` with
`name: 'alice'` still has both after an edge batch touches it as an endpoint.
Same two requests, strictly safer ordering.

The second shape works on ids that **already exist**, leaving their edges
intact, so a full bulk load costs **two requests, not N** — chunked to 1024
items each. Per-edge `MERGE` remains fine when the edge count is small.

**The two batches take differently-shaped `$rows`.** The edge batch consumes
`{src, dst}` per element; the node batch consumes `{id, name}`. Build them
separately — derive the node list from your edge list yourself:

```python
edges = [{"src": a, "dst": b} for a, b in pairs]
nodes = [{"id": i, "name": names[i]} for i in {x for e in edges for x in (e["src"], e["dst"])}]
```

## Path queries

- `algo.SPpaths` — bounded paths between one source and one target
- `algo.SSpaths` — bounded paths from one source
- `algo.MSpaths` — many sources and targets resolved in one request

```cypher
CALL algo.SPpaths({
  sourceNode: 1,
  targetNode: 5,
  relTypes: ['FOLLOWS'],
  relDirection: 'outgoing',
  maxLen: 3,
  pathCount: 1
})
YIELD path
RETURN path
```

`sourceNode` and `targetNode` are non-negative vertex ids. `relDirection` is
`outgoing`, `incoming`, or `both`. `relTypes` accepts **several types at once**
(`relTypes: ['FOLLOWS', 'KNOWS']`) — unlike `MATCH` patterns, which take exactly
one; the procedures are the only way to traverse across types in one request. **`maxLen` counts edges, not nodes** — a
three-edge path needs `maxLen: 3`; at `maxLen: 2` it returns no rows, which is
indistinguishable from "no path exists". `pathCount: 0` returns every path tied at the
minimum weight; a positive value caps the count. Paths are simple — no vertex is
revisited. With weights, add `weightProp`, `costProp`, and `maxCost`.

`algo.MSpaths` replaces client-side fan-out: give it a label, a property, and a
list of values, and it evaluates all pairs server-side. Use `pairwise: true` to
drop self and symmetric duplicates, and `resultLimit` to bound the response.
Note the consequence on a directed graph: with `pairwise: true` each unordered
pair is evaluated once, so N values yield at most C(N,2) results and A→B and
B→A are not distinguished. Pass `pairwise: false` — and set it explicitly, since
the default is unspecified — to evaluate ordered pairs in both directions.
Self-pairs (A→A) are dropped either way, so five values yield **at most** 20
ordered results rather than 25 — fewer when some pairs have no path.

A complete call — `sourceLabel` and `sourceProperty` are **required**, and the
value lists are inlined because list parameters are rejected (see below):

```cypher
CALL algo.MSpaths({
  sourceLabel: 'Person', sourceProperty: 'name',
  sourceValues: ['alice', 'bob', 'carol'],
  targetValues: ['alice', 'bob', 'carol'],
  relTypes: ['FOLLOWS'], relDirection: 'outgoing',
  maxLen: 3, pathCount: 1, pairwise: true, resultLimit: 200
})
YIELD path
RETURN path
```

A missing key surfaces as `missing OpenCypher query parameter $sourceLabel` —
misleadingly phrased, since these are map keys, not `$parameters`.

**`sourceValues` and `targetValues` must be lists of strings.** Passing numbers
fails with `sourceValues must be a list of strings`, so `MSpaths` selects by a
string property (a name or key), not by numeric node id. Use `algo.SPpaths` or
`algo.SSpaths` when you have ids.

Selector semantics, verified:

- A value matching **no node** contributes zero paths **silently** — a typo'd
  name is indistinguishable from "no path exists". Validate your values against
  a label-anchored lookup first if that distinction matters.
- A value matching **several nodes** (duplicate property values) fans out to
  every matching node.
- `pathCount` applies **per source-target pair**; `resultLimit` caps the
  **total** paths in the response.
- **A pair with no path within `maxLen` is simply absent** from the results —
  not an empty entry. So N values do not guarantee N×(N−1) rows: verified, four
  values whose twelve ordered pairs include only four reachable ones returned
  **4** paths. Never index the response by pair position; join the returned
  paths back to your pairs by their endpoint ids.

A `path` cell uses **a third encoding**, and it is the most confusing part of
the API:

```json
{"type": "path", "value": {
  "nodes": [{"id": 3001, "labels": ["Person"], "properties": {"name": {"String": "alice"}}},
            {"id": 3002, "labels": ["Person"], "properties": {"name": {"String": "bob"}}}],
  "relationships": [{"id": null, "edge_type": "FOLLOWS", "src": 3001, "dst": 3002,
                     "properties": {}}]
}}
```

- `id` and `labels` are **plain values** — read them directly.
- `properties` are **tagged with a capitalised type name**: `{"String": "alice"}`,
  not `{"type": "string", "value": "alice"}`. This is a different convention
  from every other cell in the API. Unwrap with
  `next(iter(prop.values()))` or match on the capitalised key.
- `relationships[].id` is **`null` in practice**. Do not build anything on edge
  ids obtained from a path; use `src`, `dst`, and `edge_type`.

Over Bolt the same result arrives as a native `PATH` object and none of this
applies.

**Scalar** parameters work inside a procedure's argument map:

```json
{"query": "CALL algo.SPpaths({sourceNode: $s, targetNode: $t, relTypes: ['FOLLOWS'], relDirection: 'outgoing', maxLen: $m, pathCount: 1}) YIELD path RETURN path",
 "parameters": {"s": 1, "t": 5, "m": 3}}
```

**List parameters do not** — `sourceValues: $names` is rejected with
`composite parameter $names is only supported as an UNWIND input`. This bites
exactly where lists matter most: `MSpaths` takes lists of values, and they must
be **inlined as Cypher literals**, with quotes escaped by hand:

```python
def cypher_strings(values):
    return "[" + ", ".join(
        "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"
        for v in values) + "]"
```

Treat the inputs as untrusted when you do this — inlining is exactly the
injection surface parameters exist to avoid.

## Read consistency

| Mode | Behaviour | Use when |
|---|---|---|
| `causal` (default) | Serves from the node's current durable view; refreshes only if your bookmark needs it | Almost always |
| `strong` | Refreshes from object storage before pinning the snapshot | You must observe every write committed before this call |

Over HTTP set `"consistency": "causal"` or `"strong"` in the body. Over Bolt,
pass `consistency` in `RUN` metadata or `turbolay.consistency` in transaction
metadata.

`strong` costs an object-store round trip on every call. Do not reach for it by
default, and it is meaningless on a mutation — a write's acknowledgement is
already its commit point.

**Read-your-writes across nodes** is what bookmarks are for: keep the `bookmark`
string from a write response and send it back in the next read's **`bookmark`**
field (singular — `bookmarks` is silently ignored, see
[Pagination](#pagination)):

```json
{"cell_id": "cell-0", "query": "MATCH …", "bookmark": "sgk:1:…"}
```

The reader waits until it has caught up to that position. Cheaper than
`strong`. A malformed bookmark is rejected, so a garbage value fails loudly —
only the wrong *field name* fails silently. `bookmark` and
`consistency: "strong"` may be combined in one request, but you rarely need
both: a bookmark guarantees you see *that specific write*, while `strong`
guarantees you see *everything committed before the call*. Use the bookmark when
you are chasing your own write, `strong` when you need a global fresh read, and
both only when you need both guarantees at once.

## Pagination

Set `page_size` in the request; **the maximum is 4096** and `0` is rejected, with
64 a reasonable starting point. A non-null `next_cursor` means more rows
remain.
To fetch the next page, **resend the entire original request** — `cell_id`,
`query`, `parameters`, and `page_size` — plus `cursor` (the `next_cursor` you
received) and `query_id` (from the same response):

```json
{"cell_id": "cell-0", "query": "MATCH (a:Person)-[:FOLLOWS]->(b) RETURN a.id AS src, b.id AS dst",
 "page_size": 64, "cursor": 12, "query_id": "http-query-7"}
```

**Resend `page_size` on every page, and keep it constant.** It is not remembered
from the first request: omit it and the server answers **200** with *all*
remaining rows in a single response, silently defeating the pagination you asked
for. Verified — a 20-row scan paged at 5 returned 15 rows on page two when
`page_size` was dropped. Varying it mid-scan is untested; hold it fixed.

**Resend `parameters` too.** Omitting them fails loudly with
`missing OpenCypher query parameter $src`, so this one at least cannot pass
unnoticed.

`cursor` alone is rejected with `result cursor does not belong to this query`;
the pair identifies the server-held cursor.

`query_id` is also stable across pages — resend the one from any page of the
same scan.

**`next_cursor` is a consuming iterator, not a position. Read this before
writing any retry logic.**

The *value* does not change between pages of one scan — a twelve-row scan paged
at four returned `8` on every page — but **each use advances the server's
iterator by one page**. The number itself is an opaque handle: it is not a row
offset or a page index, it is unrelated to `page_size`, and the same query run
twice gets different values (`1`, then `2`). Never compute with it. Sending the same cursor value twice does not re-fetch a
page; it fetches the *next* one:

```text
page 1              -> rows 1-4    next_cursor = 8
send cursor 8       -> rows 5-8    next_cursor = 8
send cursor 8 again -> rows 9-12   next_cursor = null   <- NOT rows 5-8 again
```

Three consequences:

- **A page fetch is not idempotent and not retryable.** If a request times out
  or you retry it for any reason, you silently skip a page and your total comes
  up short with no error. Fetch each page exactly once; to recover from a
  failure, restart the scan from the beginning.
- Do **not** treat a repeated cursor value as a stuck loop — that is normal, and
  a guard built on it fails on the second page of every scan.
- Loop until `next_cursor` is `null`. If you want a stall check, compare page
  **contents**, or bound the page count.

Two different scans get different handles, so the value is per-scan and carries
no meaning across scans.

**The last page carries rows and a null cursor together** — there is no trailing
empty page to fetch.

**A drained or dead cursor fails with yet another message**:
`result cursor is unknown or expired`. You will see it if you keep a cursor
after its scan completed — including when a dropped `page_size` silently drained
the scan in one response (see below).

Paged reads stay pinned to one storage snapshot, so rows do not shift underneath
a long pagination.

**Misspelled request fields fail silently.** Unknown top-level fields —
`page_token`, `offset`, `bookmarks`, anything — are ignored with a **200**, not
rejected. A wrong guess at the pagination field therefore re-serves page 1
forever: an infinite loop with no error. The same applies to the bookmark field.
(`consistency` is the exception — an invalid value is rejected.) The symptom is
**identical page contents**, not a repeated cursor value; check the field
spelling against the example above.

For large result sets prefer `Accept: application/x-ndjson`, which streams a
header, typed row records, and a final summary.

## Limits that look like bugs

Queries run under enforced budgets: scanned edges, intermediate rows, result
vertices, response bytes, and a wall-clock deadline. **Fewer rows than expected
may mean a budget was hit, not that the data is missing.**

- Bound variable-length traversals as tightly as the task allows; `*1..3` and
  `*1..10` can differ by orders of magnitude in work.
- Prefer one `algo.MSpaths` call over N client-side path queries.
- Cancel long queries with
  `POST /v1/graphs/{graph_id}/queries/{query_id}/cancel`, using the `query_id`
  from the response. It only affects a query that is still running; against a
  finished or unknown id it returns **400** with
  `no active query with id … was cancelled`.

## Transactions

Auto-commit only. Bolt `BEGIN`, `COMMIT`, and `ROLLBACK` are rejected. Each
`RUN` or HTTP request commits on its own. Design writes to be idempotent — see
[Identity and idempotency](#identity-and-idempotency) — rather than relying on
rollback.

## Checklist before you ship

- `id` is an upsert key; nodes with the same `id` are the same node
- Use `MERGE` or `UNWIND … CREATE` for anything re-runnable; plain `CREATE`
  duplicates relationships
- Read cells with `.get("value")` — a null cell has no `value` key
- Treat `"rows": []` on a mutation as success
- Every relationship pattern is directed and has exactly one type
- Every variable-length pattern has an upper bound; `*1..2` includes one hop
- Every `MATCH` has an id, label, or property predicate
- No `RETURN *`; name your columns
- A single-statement `MERGE` cannot be followed by `SET` (use `MATCH … SET`);
  the `UNWIND` node batch is the opposite — it **requires** `MERGE … SET n:Label`
- Single-statement writes are one-hop edge patterns only; an edgeless node needs
  the `UNWIND … MERGE … SET n:Label` batch shape
- Set properties inline in the write; relationship properties work too
- `/healthz` is on the query port, `/readyz` and `/metrics` on the admin port
- Branch on `error` versus `rows`, **and** guard the 422 case where neither is
  present
- `cell_id` is mandatory on every request
- `count(DISTINCT …)` is rejected; keep your id range clean instead
- Path `properties` use a capitalised tag (`{"String": "x"}`), unlike every
  other cell; path relationship ids are `null`
- Reading a node id gives `vertex_id`, not `integer`
- **Anchor existence checks on a label** — `MATCH (n {id: X})` returns a phantom
  row for ids that do not exist
- `count(*)` after `OPTIONAL MATCH` counts the null row; use `count(<property>)`
- Grouped aggregation omits zero-count nodes entirely — default them client-side
- `maxLen` in the path procedures counts edges
- `[:R*a..b]` revisits nodes through cycles — exclude the start node yourself
- Parameters work in write property maps, not just in `MATCH`; in procedure
  maps **scalars only** — lists must be inlined as escaped literals
- Page by resending the **whole** request — `query`, `parameters` and
  `page_size` included — plus `cursor` and `query_id`; dropping `page_size`
  silently returns every remaining row at once
- `next_cursor` is constant across pages **but each use consumes a page** — a
  page fetch is not retryable; on failure restart the scan. Never use a repeated
  cursor as a stuck-loop signal; compare page contents instead
- Give any top-N a secondary `ORDER BY` key, or ties make it non-deterministic
- `MSpaths` omits pairs with no path — join results back by endpoint id
- Batches are capped at **1024 items**; chunk to 1000
- Batch dedup is on (src, dst, **type**) — multiple relationship types are safe
- Clear an id range with batched `DETACH DELETE`; unlabelled nodes cannot be
  detected any other way
- Batch the labels **first**, then the edges — two requests, and no window in
  which nodes exist unlabelled and undetectable
- Unknown request fields are silently ignored — a misspelled `cursor` or
  `bookmark` degrades to defaults with a 200
- The read-your-writes field is `bookmark`, singular
- Batch labels **and** properties together: `SET n:Person, n.name = row.name`;
  the label half is mandatory, the properties are not
- `RETURN` takes properties and aggregates only — no arithmetic, no path bindings
- Label your nodes at seed time if you will ever need graph-wide counts
