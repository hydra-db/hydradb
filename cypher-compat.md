# Cypher support

What HydraDB accepts today, taken from the parser in `src/query/opencypher.rs`
and the path procedures in `src/query/path_procedure.rs`.

HydraDB implements a deliberate subset of OpenCypher rather than the whole
language. The subset is shaped by what an object-store-native graph engine can
execute efficiently, so patterns resolve to id lookups and typed adjacency scans.
Anything outside it is rejected at parse time with a clear reason rather than
being planned into something slow.

A statement is parsed once and cached, and only one statement per request is
accepted.

## Quick reference

| Clause | Support |
|---|---|
| `MATCH` | Yes, one or more path patterns, directed, one relationship type each |
| `OPTIONAL MATCH` | Yes for reads, rejected in mutations |
| `WHERE` | Yes, boolean combinations of property comparisons |
| `RETURN` | Yes, property projections and aggregates, with alias, `DISTINCT`, `ORDER BY`, `SKIP`, `LIMIT` |
| `WITH` | Pass-through only, no aliases and no filtering |
| `CREATE` | Yes, one or more relationship paths |
| `MERGE` | Yes, matched on id, no `ON CREATE` or `ON MATCH` |
| `SET` | Yes, properties and labels, after a `MATCH` |
| `REMOVE` | Yes, properties and labels, after a `MATCH` |
| `DELETE`, `DETACH DELETE` | Yes, after a `MATCH` |
| `UNWIND` | Yes, as a batch form driven by a parameter |
| `UNION`, `UNION ALL` | Yes for reads, arms must project the same columns |
| `CALL algo.*` | Yes, three native path procedures |
| `CREATE UNIQUE` | No |
| `RETURN *` | No |

## Reading

### Patterns

A relationship pattern carries exactly one type and a direction. Undirected
patterns are rejected.

```cypher
MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id
MATCH (n:Person) WHERE n.id = 42 RETURN n.name
MATCH ()-[e:FOLLOWS {weight: 7}]->() RETURN count(*)
```

Nodes match on `id`, optionally with a label and inline properties. A node
carrying labels or non-id properties has to be named.

A `MATCH` of a node on its own needs something to select on — an id, a label, or
a property. There is no bounded set of vertices to scan without one, so the
otherwise obvious `MATCH (n) RETURN count(*)` is refused:

```
node-only MATCH requires an id, label, or property predicate
```

This one is raised when the query runs rather than when it parses, so it is
reported against real data rather than by a syntax check.

### Variable-length paths

```cypher
MATCH (u {id: 1})-[:CHAIN*1..3]->(v) RETURN v.id ORDER BY v.id
```

The range covers every path from the minimum through the maximum, so `*1..3`
returns everything reachable in one, two or three hops rather than only those
exactly three long.

The maximum is required. `*1..` and `*` are rejected, because an unbounded
traversal on a large graph has no predictable cost. The minimum defaults to 1
when omitted, and must not exceed the maximum.

#### The source node needs an id

A variable-length pattern starts from a node whose `id` the pattern supplies,
and walks the arrow away from it. The id may be a literal or a parameter, but it
has to sit in that node's inline property map:

```cypher
MATCH (u {id: 1})-[:CHAIN*1..3]->(v) RETURN v.id
MATCH (u {id: $start})-[:CHAIN*1..3]->(v) RETURN v.id
```

Without it the query is refused when it runs:

```
variable-length MATCH requires a fixed source id
```

This makes direction load-bearing in a way a fixed-length pattern does not. An
inbound spelling puts the id on the destination and leaves the *source* unbound,
so no arrangement of the same pattern satisfies the requirement:

```cypher
// refused: the source is (x), and (x) is the node being searched for
MATCH (c {id: 5})<-[:DEPENDS_ON*1..2]-(x) RETURN x.name
```

So "everything reachable from X" is expressible and "everything that reaches X"
is not, unless the graph also carries the reverse relationship type. For the
same reason a transitive closure over a whole relation — every
`(a)-[:MANAGES*1..6]->(b)` pair, with no bound endpoint — has to be driven from
the client, one start node at a time.

### WHERE

Boolean combinations of property comparisons, using `AND`, `OR` and `NOT`.

Comparison operators: `=`, `<>`, `<`, `>`, `<=`, `>=`, and `STARTS WITH`.

```cypher
MATCH (s:Score) WHERE s.score > 3.0 RETURN s.id AS score_id ORDER BY score_id
MATCH (s:S) WHERE s.a = 1 AND (s.b > 2 OR NOT s.c = 3) RETURN s.id
MATCH (s:Source) WHERE s.thread_id STARTS WITH $prefix RETURN s.id
```

`STARTS WITH` needs a string literal or a parameter on the right.

`IN`, `ENDS WITH`, `CONTAINS` and `IS NULL` are not supported. All four are
rejected with the same message, that `WHERE` supports boolean combinations of
property comparisons.

### RETURN

Projections are `<binding>.<property>` or an aggregate. `RETURN *` is not
executable, so name what you want.

Aggregates: `count`, `sum`, `avg`, `collect`. `count(*)` is supported.
`DISTINCT` inside an aggregate argument is not, and neither is `count(DISTINCT *)`.

`ORDER BY` accepts a projected alias, `<binding>.id`, or `count(*)`, ascending
or descending. `SKIP` and `LIMIT` are supported, as is `DISTINCT` on the
projection itself.

```cypher
MATCH (s:Score) RETURN s.id AS score_id, s.score AS score ORDER BY score, score_id
MATCH (u {id: 1})-[:FOLLOWS*1..2]->(v) RETURN count(*) AS total
MATCH ({id: 1})-[:CHAIN*1..2]->(v) RETURN v.id ORDER BY v.id DESC
```

### WITH

`WITH` passes bindings through and nothing more. Every in-scope binding has to
be carried, written as bare identifiers, with no aliasing, filtering, ordering
or `DISTINCT`.

### UNION

Read queries only. Every arm must project the same column names, `UNION` and
`UNION ALL` cannot be mixed in one query, and unions cannot nest.

## Writing

Writes commit to object storage, so each statement is durable when it returns.

### CREATE

One or more relationship paths, each with a source id and a destination id.
Properties can be set on the endpoints and on the relationship.

```cypher
CREATE (u {id: 1})-[:FOLLOWS]->(v {id: 2})
CREATE (a:Entity {id: 1, name: 'alpha'})-[:RELATES {relationship_id: 'ab'}]->(b:Entity {id: 2, name: 'beta'})
```

`CREATE` cannot be followed by another clause, cannot use a variable-length
relationship, and `CREATE UNIQUE` is not supported.

### MERGE

Matches on id and creates when absent. `ON CREATE` and `ON MATCH` are not
supported, so apply properties with a following `SET` on a matched pattern
instead.

```cypher
MERGE (u {id: 1})-[:FOLLOWS]->(v {id: 2})
MERGE (u:User {id: 1, name: 'alice'})-[:FOLLOWS]->(v:User {id: 2})
```

A `MERGE` that changes nothing still commits, so an idempotent retry costs the
same as the original write.

### SET, REMOVE, DELETE

Each requires a preceding `MATCH`. Properties are written as
`<binding>.<property>`, and labels can be set or removed. The `id` property
cannot be changed or removed, because it is the identity the pattern matched on.

```cypher
MATCH (u {id: 1}) SET u.name = 'alice'
MATCH (u {id: 1}) SET u.active = true, u:Moderator
MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) SET r.since = 2021, r.weight = 7
MATCH (u {id: 1})-[r:FOLLOWS]->(v {id: 2}) DELETE r
MATCH (u:User {id: 1}) DETACH DELETE u
```

`DELETE` takes node or relationship variables. Mutations cannot be combined with
`OPTIONAL MATCH`.

## Batches with UNWIND

`UNWIND` drives a batch from a parameter, which is how many rows are written in
one round trip. The input has to be a parameter holding a list of maps, not an
inline list, and every row must carry the fields the statement reads.

Upsert vertices, create relationships between matched vertices, and delete:

```cypher
UNWIND $rows AS row MERGE (n {id: row.vertex}) SET n:Source, n.source_id = row.source_id, n.active = row.active

UNWIND $rows AS row
  MATCH (s:Entity {id: row.source_vertex}), (d:Entity {id: row.destination_vertex})
  CREATE (s)-[:RELATES {id: row.relationship_vertex, relationship_id: row.relationship_id}]->(d)

UNWIND $rows AS row
  MATCH (s:Entity {id: row.source_vertex}), (d:Entity {id: row.destination_vertex})
  MERGE (s)-[r:RELATES {id: row.relationship_vertex}]->(d)
  SET r.relationship_id = row.relationship_id, r.chunk_id = row.chunk_id

UNWIND $rows AS row MATCH ()-[r:RELATES {chunk_id: row.chunk_id}]->() DELETE r
UNWIND $vertices AS row MATCH (n {id: row.vertex}) DETACH DELETE n
```

A vertex upsert has to be `MERGE` by id followed by `SET`. Folding the other
properties into the `MERGE` pattern, as in
`MERGE (n:Source {id: row.vertex, source_id: row.source_id})`, is rejected:
the pattern is the identity being matched on, so writing extra properties
into it would rewrite what it matched.

The batch forms are narrow by design, and the rules are worth knowing before you
write one:

- The list comes from a parameter. An inline literal list is rejected.
- Ids read fields from the row map, so `{id: row.vertex}`, and the alias has to
  be the one the `UNWIND` bound.
- One relationship pattern per batch, one hop, directed.
- `UNWIND ... CREATE` and `UNWIND MATCH ... CREATE` cannot be followed by
  another clause, and an `UNWIND MATCH` must end in `RETURN` or `DELETE`.
- `UNWIND MATCH` does not take `OPTIONAL`, hints, or `WHERE`.

Batches run through the client service that the Bolt server uses, because a
parameter holding a list of maps is a transport-level type. The in-process shard
API carries scalar parameters only, so `execute_cypher` on a shard rejects every
`UNWIND` form above. The message it gives is about row execution rather than
about batching, which is worth knowing before you spend time on the query
itself: the statement is fine, the entry point is wrong.

## Path procedures

Three native procedures, each called with a config map, a `YIELD`, and a
`RETURN` that may only reference yielded columns.

| Procedure | Meaning |
|---|---|
| `algo.SPpaths` | Paths between a single source and a single target |
| `algo.SSpaths` | Paths from one source |
| `algo.MSpaths` | Paths from many sources |

```cypher
CALL algo.SPpaths({sourceNode: $source, targetNode: $target, relTypes: ['RELATES'],
                   maxLen: 3, relDirection: 'both', pathCount: $count})
  YIELD path, pathWeight, pathCost
  RETURN path, pathWeight, pathCost

CALL algo.SSpaths({sourceNode: 7, relTypes: ['RELATES'], maxLen: 11}) YIELD path RETURN path
```

Config keys include `sourceNode`, `targetNode`, `sourceLabel`, `sourceProperty`,
`sourceValues`, `targetLabel`, `targetProperty`, `targetValues`, `relTypes`,
`relDirection`, `maxLen`, `pathCount`, and weight and cost properties with an
optional maximum cost. Setting `targetLabel` or `targetProperty` requires
`targetValues` as well.

Yieldable columns are `path`, `pathWeight` and `pathCost`. `RETURN` may only
name columns that were yielded.

Unlike a plain `MATCH`, these are the way to get whole paths back rather than
endpoint projections.

## Values and parameters

Property values are integers, floats, booleans and strings. Unary plus and minus
apply to numbers only. Node ids are non-negative integers.

Parameters are written `$name`:

```cypher
MATCH (u {id: $missing})-[:FOLLOWS]->(v) RETURN v.id
MATCH (s:Score {score: $score}) RETURN s.id AS score_id ORDER BY score_id
```

Scalar parameters work everywhere. A parameter holding a list of maps is only
accepted as `UNWIND` input, and only through the client transport.

## Not supported

Rejected at parse time, with the reason in the error:

- `RETURN *`, and projections other than `<binding>.<property>` or an aggregate
- `CREATE UNIQUE`, and `ON CREATE` or `ON MATCH` on `MERGE`
- Undirected relationship patterns, and patterns with more than one type
- Unbounded variable-length traversal, `*` or `*1..`
- `WITH` that aliases, filters, orders, or drops a binding
- Aggregate arguments marked `DISTINCT`, and `count(DISTINCT *)`
- Aggregates beyond `count`, `sum`, `avg` and `collect`, so no `min` or `max`
- `IN`, `ENDS WITH`, `CONTAINS` and `IS NULL` in `WHERE`
- `MATCH` hints
- Nested unions, mixed `UNION` and `UNION ALL`, and unions containing writes
- Variable-length relationships in `CREATE` and `MERGE`
- More than one statement per request

Two are refused when the query runs instead, so they parse cleanly and are only
reported once a statement reaches data:

- A node-only `MATCH` carrying no id, label, or property predicate
- A variable-length `MATCH` whose source node has no `id`

## Checking a query without running it

`EXPLAIN` is available through the shard API as `explain_opencypher_rows`, which
returns the plan for a read query. A statement that the parser rejects fails
there too, with the same message, so it is a cheap way to check a query before
it goes near data.
