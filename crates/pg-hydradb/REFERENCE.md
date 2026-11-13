# pg_hydradb reference

This is the full reference. For a quick start, see [README.md](README.md).

## What it is

`pg_hydradb` runs graph queries inside PostgreSQL, over tables your application
already owns. PostgreSQL still handles storage, transactions, concurrency,
permissions and recovery. The extension keeps a copy of the graph's links in
two PostgreSQL tables and returns results as JSON. It does not run `graph-node`
and does not move your tables anywhere else.

It is a preview, not a new storage engine. How it works:

- **Triggers keep the copy current.** Triggers on your tables update
  `hydradb.graph_vertices` and `hydradb.graph_adjacency` **inside the same
  transaction** as your write. Writing one edge adds or removes one row.
- **The copy is ordinary PostgreSQL tables** with B-tree indexes, so WAL, crash
  recovery, replicas, `pg_dump` and transaction visibility all work as for any
  table.
- **Queries read only what they walk.** A query reads the part of the copy it
  needs, one layer of the walk at a time, through shared buffers. Nothing is
  cached per connection, and nothing is rebuilt after a write or a restart.
  Each connection only remembers a graph's description (which tables and
  columns it uses). It forgets it when PostgreSQL reports a change to one of
  those tables, and still checks the registration and the caller's privileges
  on every call.
- **Queries never read your tables in full.** They read only the copy. So a
  graph the copy cannot represent is refused at `create_graph`: sources that
  are not ordinary permanent tables, tables in an inheritance tree, tables
  whose row-level security applies to the extension owner, and columns whose
  text form depends on session settings.
- **A query that can't be answered says why,** and how to fix it. That happens
  when the graph is paused for `hydradb.refresh_graph`, when one of its
  triggers is disabled, when a source table has become unfit, or when
  row-level security applies to the caller.

## Build and test

PostgreSQL **16.15**, **17.11** and **18.6** are tested, chosen with the Cargo
features `pg16`, `pg17` and `pg18`. All three pass `pg-check`, `pg-install` and
`pg-test`: 16 on macOS, 17 and 18 on Debian trixie (arm64) with the
apt.postgresql.org packages. CI runs PostgreSQL 18.

Put the `pg_config` of the version you want on `PATH`. Its headers, server
programs and install folders must all belong to that version.

From the repository root:

```sh
just test-pg-graph
just check-pg-graph
just pg-build 18
just pg-check 18
just pg-install 18
just pg-test
```

- `pg-build` builds the optimized release version.
- `pg-install` copies the library, the control file and the SQL script into the
  folders `pg_config` reports. It needs write access to them. It does not run
  `CREATE EXTENSION` in any database.
- The PostgreSQL adapter (built with pgrx) is its own Cargo workspace. The
  three graph crates are ordinary members of the main workspace and don't
  depend on PostgreSQL.

`pg-test` needs Python 3 (standard library only) and the installed extension.
It:

- takes `initdb`, `pg_ctl` and `psql` from `pg_config --bindir`;
- creates a new temporary folder (`mktemp`) and a throwaway PostgreSQL server in it, with
  TCP turned off, reachable only through its own Unix socket;
- never connects to or changes your existing databases;
- stops the server and deletes the folder when it ends, even on failure.

Run it as a non-root user. `PG_CONFIG` and `PYTHON` choose which programs it
uses.

The tests check:

- walks up to depth 10, directions and edge types, parallel edges and
  self-loops, several seeds, shortest and listed paths, limits and invalid
  options;
- the reason every refused graph and query gives;
- that the copy matches the tables after inserts, updates, key changes,
  deletes, cascades, truncate, refresh and random write sequences, including
  when other triggers on the table write to it;
- that a transaction sees its own writes at every isolation level, including
  savepoint rollback;
- that writers of one graph don't wait for each other;
- read-only transactions, revoked privileges, row-level security, and that
  views are refused as sources;
- with several sessions open at once: uncommitted changes stay invisible,
  committed ones appear, repeatable-read snapshots stay old and read-committed
  ones move forward;
- recovery of the copy from WAL after an immediate stop and restart.

## Registering a graph

An administrator installs the extension and registers the tables:

```sql
CREATE EXTENSION pg_hydradb;
CREATE TABLE vertices (id bigint PRIMARY KEY, sort_key text NOT NULL);
CREATE TABLE edges (
    id bigint PRIMARY KEY,
    source bigint NOT NULL REFERENCES vertices,
    target bigint NOT NULL REFERENCES vertices,
    kind text NOT NULL
);
INSERT INTO vertices VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO edges VALUES (10, 1, 2, 'link'), (20, 2, 3, 'link');
SELECT hydradb.create_graph('example',
  '[{"label": "vertex", "table": "public.vertices", "key": "id", "sort_key": "sort_key"}]',
  '[{"label": "edge", "table": "public.edges", "key": "id",
     "source": "source", "source_label": "vertex",
     "target": "target", "target_label": "vertex", "kind": "kind"}]');
```

If your client has autocommit turned off, commit before querying.

**Who can register.** Registering a graph, and removing it with
`hydradb.drop_graph('example')`, are for administrators by default.
`create_graph`, `refresh_graph` and `drop_graph` run as the extension owner. So
even a role that has been granted `EXECUTE` on them can only use them on tables
its login role (`session_user`) owns, or whose owner it is a member of. Adding
triggers to a table is otherwise only its owner's right.

### Several vertex and edge tables

A graph can use any number of vertex and edge tables. You say which column
plays each role:

```sql
SELECT hydradb.create_graph('org',
  '[{"label": "person",  "table": "public.people",    "key": "id", "sort_key": "name"},
    {"label": "company", "table": "public.companies", "key": "slug"}]',
  '[{"label": "works_at", "table": "public.employment", "key": "id",
     "source": "person_id",    "source_label": "person",
     "target": "company_slug", "target_label": "company"}]');
```

The rules:

- **Tables.** Names must include the schema and must be ordinary tables. Views,
  materialized views and partitioned tables are refused. Register the
  underlying tables instead, and filter rows with row-level security or in the
  query that joins the results.
- **Permanent tables only.** A registration outlives the session; a temporary
  table does not.
- **How many.** A graph has at least 1 and at most 64 vertex tables, and at
  most 64 edge tables. Vertex labels must be unique in a graph, and so must
  edge labels. Every `source_label` and `target_label` must name one of the
  graph's vertex tables.
- **Fields.** Each declaration takes only the fields shown, plus `sort_key` for
  a vertex table and `kind` for an edge table. Every named column must exist.
  A graph name can be registered once; `drop_graph` it to register it again.
- **Keys.** A `key` must be an integer, `text`, `varchar`, `char` or `uuid`
  column, with a deterministic collation, and unique within its table. Keys
  are compared by their printed text, so a type or collation that treats
  differently printed values as equal is refused: `numeric` (1 and 1.00),
  `citext`, or a case-insensitive ICU collation. Otherwise an edge could miss
  the vertex it points to.
- **Edge ends.** Each `source` or `target` column points at the key of exactly
  one vertex table, and must have the same type as that key (any two integer
  types are fine). Every query checks the types again, so an end whose type
  was changed after registration gives an error, not an edge to the wrong
  vertex.
- **Uniqueness.** Keys must be unique within their table. Edges keep their own
  keys, so parallel edges and self-loops stay separate.
- **No NULLs.** Every declared column is required and must not be NULL.
- **Edge types.** An edge's type, which `edge_kind` and `edge_kinds` match, is
  its table's label, unless the declaration names a `kind` column.
- **Both ends must exist.** Both ends of every edge must exist in the vertex
  table they point to, as visible to the query. If they don't, the query fails
  rather than silently skipping the edge.
- **Which results a limit keeps.** When a limit cuts a layer short, the
  vertices and edges kept are the first by `sort_key` (empty when not
  declared), then table label, then key: integers by value, everything else
  byte by byte. Sort keys are loaded only for a layer a limit cuts.
- **Order of results.** Results are not sorted by `sort_key`. Within one depth
  they come back in internal id order, so sort them yourself if the order
  matters. When several shortest paths tie, any of them may be returned.

### Seeds and keys

Every function takes its starting vertices ("seeds") as a JSON object that maps
vertex labels to one key or a list of keys. Results name vertices and edges the
same way, by label and key:

```sql
SELECT hydradb.reachable_vertices('org', '{"person": [7, 8], "company": "acme"}', '{"max_depth": 3}');
-- items: [{"label": "person", "key": 7, "depth": 0}, {"label": "company", "key": "acme", "depth": 1}, ...]
```

Keys keep their type:

- **Integer keys** are seeds and results as JSON numbers: `7`, not `"7"`.
- **Every other key type** is the text PostgreSQL prints for it, as a JSON
  string that must match exactly. UUIDs are lowercase.
- A seed of the wrong JSON type is an error, and so is a seed the graph doesn't
  have.

## Walking the graph

The two walk functions are `STABLE`, `SECURITY INVOKER` and parallel-unsafe:

```sql
SELECT hydradb.reachable_vertices('example', '{"vertex": [1]}',
    '{"max_depth":10,"direction":"outgoing","edge_kind":"link"}'::jsonb);
SELECT hydradb.reachable_edges('example', '{"vertex": [1]}',
    '{"max_depth":10,"direction":"both"}'::jsonb);
```

- `reachable_vertices` includes the seeds at depth 0 and returns
  `items: [{"label":"vertex","key":1,"depth":0}, ...]`.
- `reachable_edges` returns `items: [{"label":"edge","key":10,"depth":1}, ...]`.
  Each distinct edge appears, and a self-loop appears once.

For paths between vertices, see [Paths](#paths).

Each response also has:

- **`truncated`:** why the walk stopped early, if it did: `depth`, `vertices`,
  `edges`, `edge_visits` or `result_bytes`. Always check it; don't assume the
  answer is complete.
- **`stats`:** `visited_vertices`, `edge_visits`, `returned_edges` and
  `result_bytes`. `result_bytes` is an estimate of memory used, not the size
  sent to the client.
- **`storage`:** `vertices`, `edges`, `estimated_bytes` and `lookups`: what the
  query read from the copy, meaning the vertices and edges it reached. The
  byte figure is what counts against the budget below, not the connection's
  total memory or every JSON allocation.

### Options and limits

Unknown fields, wrong types and values above the hard limits are errors.

- `direction` is `outgoing` (default), `incoming` or `both`.
- `edge_kind` keeps only edges of that exact type.

Each limit bounds a different kind of work, so a small result limit doesn't let
a query load an unlimited amount of data.

| Option | Default | Hard limit |
| --- | ---: | ---: |
| `max_depth` | 5 | 255 |
| `max_vertices` | 20,000 | 20,000 |
| `max_edges` | 5,000 | 5,000 |
| `max_edge_visits` | 1,000,000 | 10,000,000 |
| `max_result_bytes` | 8 MiB | 64 MiB |
| `max_sources` | 32 | 128 |
| `max_snapshot_bytes` | 128 MiB | 512 MiB |

- **Must be positive:** the vertex, source and snapshot limits.
- **`max_snapshot_bytes`** limits what a query loads from the copy. Each
  vertex counts 128 bytes and each edge 192, plus the length of its key, sort
  key or type (the table label when there's no `kind` column). So it also
  limits how many rows a query loads. Going over it is an error that says how
  many vertices and edges had been loaded.
- **Stopping early.** A walk that stops early lists why in `truncated`.
  `depth`, `vertices` and `edges` are your own limits; `edge_visits` and
  `result_bytes` are work and size ceilings.
- **`strict`.** With `{"strict": true}`, a walk stopped by a ceiling raises an
  error instead of returning a partial result. You then get either an answer
  within your own limits, or an error. The default is `false`.
- **Result size.** The whole JSON result is checked against
  `max_result_bytes` too. A result that doesn't fit raises an error rather than
  returning something oversized.
- **Cancelling.** Cancelling the statement in PostgreSQL stops loading and
  walking.

**Field size.** Keys, edge ends, `sort_key` and `kind` values are limited to
64 KiB each. Larger values are rejected, never cut short: SQL keeps oversized
text out of what it hands the extension, and the Rust code checks the length
before decoding. This applies to the graph's own columns, not to page content
or other columns you join afterwards.

**What the limits cover.** They count data kept from the copy, with temporary
buffering bounded by the remaining budget plus at most one extra row. They don't
bound a row-level security policy's plan, a function such a policy calls, or the
connection's exact memory use. PostgreSQL's own limits and statement timeouts
still apply.

## Paths

`paths_from` lists paths from each source to any vertex. `paths_between` lists
paths from each source to each target. Both work on the same registered graph:

```sql
SELECT hydradb.paths_between('example', '{"vertex": 1}', '{"vertex": 3}',
    '{"edge_kinds":["link"],"max_depth":4,"direction":"both"}'::jsonb);
SELECT hydradb.paths_from('example', '{"vertex": [1, 2]}', '{"max_depth":2,"paths_per_pair":10}'::jsonb);
SELECT hydradb.paths_between('example', '{"vertex": [1, 2, 3]}', '{"vertex": [1, 2, 3]}',
    '{"max_depth":2,"paths_per_pair":0,"unordered_pairs":true,"balance_parallel_edges":true,"max_paths":10}'::jsonb);
```

- Sources and targets are seeds, as above. A key the graph doesn't have is an
  error.
- Each returns
  `items: [{"vertices":[{"label":"vertex","key":1}, ...],"edges":[{"label":"edge","key":10}, ...]}, ...]`,
  plus `stats` (`edge_visits`, `adjacency_vertices`, `result_bytes`) and
  `storage`, as above.
- `edges` are listed in path order. Join them to their edge tables for
  direction and other columns.
- Paths are simple: no vertex appears twice. A source that is also a target
  gets the path with no edges.

Options:

| Option | Default | Meaning |
| --- | --- | --- |
| `edge_kinds` | every type | Edge types to follow |
| `direction` | `outgoing` | `outgoing`, `incoming` or `both` |
| `max_depth` | 16 | Longest path, in edges; at most 255 |
| `paths_per_pair` | 1 | Paths kept per source and target (or per source, without targets), shortest first; 0 keeps every shortest path |
| `max_paths` | 100,000 | Total paths returned |
| `unordered_pairs` | `false` | With targets, one pair per two vertices, from the smaller key |
| `balance_parallel_edges` | `false` | With `unordered_pairs`, share `max_paths` across routes, so a route with many parallel edges doesn't take it all |
| `max_candidates` | 250,000 | Paths and vertices the search may hold at once |
| `max_edge_visits` | 1,000,000 | Edges the search may look at; at most 10,000,000 |
| `max_result_bytes` | 8 MiB | Result size; at most 64 MiB |

How the search works:

- **Shortest first.** The graph has no weights, so shorter paths come first.
- **One path per pair** (targets given, `paths_per_pair` 1): the search walks
  outward layer by layer and keeps one shortest path per pair. Where several
  edges join the same two vertices, it takes the one with the lowest key. It
  lists no other paths, so deep pairs and dense graphs only cost what the walk
  reaches.
- **Otherwise** it lists simple paths, and every combination of parallel edges
  counts as its own path. The number of paths can grow very fast with
  `max_depth`, so `max_candidates` and `max_edge_visits` turn an oversized
  request into an error rather than a cut-off answer.
- **Result size.** `max_result_bytes` is counted as each path is listed, and
  going over is also an error rather than a cut. The final JSON is checked
  against it again.
- **Ties.** Paths of the same length are ordered by table label and key, then
  by edge, not by `sort_key`.
- **What gets loaded.** A path search loads only the edge lists it scans: with
  targets, those on the walk back from the targets; otherwise, those on the
  walk out from the sources. A path's edges come from those lists, so the
  search doesn't load the edges of every vertex it reached.

## How the graph stays in sync

### Visibility

A graph query sees what the statement calling it sees (its MVCC snapshot).
Every lookup is read-only (`read_only` SPI), so one call never mixes snapshots, and a graph query
joined with ordinary SQL in the same statement sees the same data as that SQL.

A transaction sees its own uncommitted inserts, updates and deletes, including
those in an open savepoint but not those rolled back to one, at every isolation
level. Other sessions see a change once it commits, as with any table.

### The triggers

When `create_graph` registers a graph, it checks that the copy can represent
it. That means every source is an ordinary permanent table without row-level
security or inheritance, and every key, edge end, sort key and kind column is
`int2`/`int4`/`int8`, `text`/`varchar`/`bpchar` or `uuid`. Their text form must
not depend on session settings. It then:

1. installs four statement-level `ENABLE ALWAYS` triggers on each table;
2. fills the copy from the current rows. Writes to the tables are blocked until
   its transaction ends.

The triggers run inside the writing transaction, including writes made in
replica mode (for example by logical replication):

- **INSERT and DELETE:** applied from the statement's list of changed rows (its
  transition table), as one set.
- **UPDATE:** compares the graph's columns in the old and new rows, removes the
  old rows and adds the new ones that differ, and leaves rows whose graph
  columns didn't change alone. It runs on every UPDATE, not only those that
  name a graph column, so a change made by a `BEFORE UPDATE` trigger is seen
  too.
- **TRUNCATE:** removes the table's rows from the graph.

How long writes take with these triggers is under
[Optional benchmarks](#optional-benchmarks).

### Other triggers on your tables

Your tables can have other triggers, including ones that write back to the same
table. PostgreSQL runs a table's `AFTER ROW` triggers, and any statement they
run, before pg_hydradb's once-per-statement trigger. So by the time pg_hydradb
runs, another trigger may have changed, deleted or re-inserted rows the
statement wrote, and the statement's list of changed rows no longer matches the
table.

pg_hydradb handles that. When a table has an AFTER trigger other than its own
and the read-only foreign key checks, or a rule:

- **Examples:** your own trigger, or a foreign key action such as
  `ON DELETE CASCADE`.
- **What pg_hydradb does:** it uses the statement's changed rows only to learn
  which keys were touched, then reads those keys from the table as it is at
  that moment. The copy then matches the table, whatever ran in between.

Tables without such triggers use the statement's changed rows directly, as
above. That reads nothing else, so under `SERIALIZABLE` it doesn't make writers
of different rows conflict with each other.

### What a table must have

The triggers must never reject a write your table accepts. So `create_graph`
(and `refresh_graph`, again) checks the following with
`hydradb.check_mirrorable`, and refuses the graph if any is missing. The error's hint is the `ALTER TABLE` that fixes it.

- **NOT NULL:** every key, sort key, edge end and kind column is `NOT NULL`.
- **A unique index on each key:** one column, not partial, not on an
  expression. A primary key counts.
  - An edge key's index must not be deferrable. A deferred check would let a
    statement hold a duplicate edge, which the copy can't store.
  - A vertex key's index may be deferrable, because the triggers count a
    duplicate vertex until commit removes it.
- **No generated columns.** On PostgreSQL 17, `ALTER COLUMN … SET EXPRESSION`
  recomputes a generated column's values without firing triggers. An event
  trigger can't tell that apart from adding a column with a default, so the
  change could be neither copied nor refused. Use an ordinary column, filled by
  the application or a `BEFORE` trigger, instead.
- **A foreign key on each edge end.**
  - **Shape:** each edge `source` and `target` column needs its own foreign key
    to the key of the vertex table it names: one column, enforced, not
    deferrable. A `NOT VALID` one counts, since it still checks every new write.
  - **Why:** without one, an edge can point at a vertex that another
    transaction is still creating. Its trigger then waits for that transaction
    to give the vertex an id, and two transactions that each link to the
    other's new vertex deadlock.
  - **With one:** such a link waits for the vertex row, or fails, before any
    trigger runs, as it would without the extension.
  - PostgreSQL itself requires the vertex key a foreign key points at to be
    non-deferrable.

### Long keys

Keys and edge ends are stored in B-tree index entries, which hold about 2.7 kB.
A write never fails because of that:

- **What happens:** when a text key or edge end longer than 2,000 bytes is
  written, the trigger lets the write through. It records the reason in
  `hydradb.graphs.needs_refresh` with a warning and stops updating the graph.
- **Effect on queries:** they fail with that reason, so the graph is unavailable
  until the value is fixed. Keep keys short, or limit them with a `CHECK`.
- **Fixing it:** once the value is shortened or removed,
  `hydradb.refresh_graph` rebuilds the copy and clears the pause. While the
  value is still there, the refresh refuses and names it.
- **Already there:** `create_graph` refuses a table that already holds such a
  value.
- **Which keys:** integer and uuid keys are never checked. Registration warns
  about a text key column with no length limit.

### Vertex ids, missing vertices and concurrent writers

- **Vertex ids.** Each vertex gets a permanent internal id (`vid`) the first
  time its key appears, from a vertex row or from an edge that names it.
  `graph_vertices.rows` counts the table rows with each key.
- **Missing vertices.** The foreign keys stop an edge from naming a vertex that
  doesn't exist. The exceptions are writes in replica mode, which skip foreign
  key checks but still fire the graph's triggers, and rows older than a
  `NOT VALID` foreign key.
- **Bad keys.** A query that reaches a key with no row (a missing vertex) or
  with several rows (a duplicate key) fails.
- **NULLs.** A NULL key, edge end, sort key or kind fails the write that
  stores it.
- **Concurrent writers.** Writers of different edges share no rows and take no
  graph-wide lock, so they don't wait for each other at any isolation level.
  Two writers that both create the same new vertex key wait for each other, as
  with any unique key.

## Schema changes

Event triggers (`pg_hydradb_refuse_source_drops` and
`pg_hydradb_check_altered_sources`) check schema changes to a graph's tables.
A change the graph depends on is refused, much as PostgreSQL refuses to drop a
column a view uses. The error gives the reason and the way round it: drop the
graph, make the change, register the graph again.

### Refused

After every `ALTER TABLE`, `CREATE TABLE` and `ALTER TRIGGER`, the check looks
at what the command left behind, not at what the command was. So kinds of
change nobody listed are caught too. Refused:

- **Any change to a graph column.** `create_graph` records each column's type
  with its length, collation, nullability and whether it's generated. A command
  that leaves any of that different, or the column missing by name, is
  refused. That covers:
  - renaming the column;
  - `ALTER COLUMN … TYPE` to another type or length;
  - `COLLATE`;
  - `DROP NOT NULL` (or, from PostgreSQL 18, dropping its not-null constraint).

  Statistics, storage, comments and indexes on the column don't count.
- **Anything that makes a table unfit:**
  - joining an inheritance tree, as a parent (`CREATE TABLE … INHERITS`) or a
    child (`ALTER TABLE … INHERIT`);
  - `SET UNLOGGED`;
  - row-level security that applies to the extension owner.
- **`ALTER TRIGGER` on a pg_hydradb trigger,** which the graph finds by name.
- **Weakening an edge foreign key:** making it deferrable or, from
  PostgreSQL 18, not enforced.

Allowed:
- adding columns, and changing columns the graph doesn't use;
- constraints and indexes;
- storage settings;
- renaming the table.

### Refused drops

- **The table,** directly or through `DROP SCHEMA … CASCADE`.
- **A graph column,** even when the same `ALTER TABLE` adds a column with that
  name back, since the triggers never saw its values.
- **A key's last qualifying unique index:** a vertex key's last unique index of
  its own, or an edge key's last non-deferrable one, through `DROP INDEX` or
  `ALTER TABLE … DROP CONSTRAINT`. The table would then accept a duplicate key.
  Another such index keeps the drop allowed.

**`DROP INDEX CONCURRENTLY`.** A concurrent drop disables the index in steps
that are committed before the drop finishes, so a refusal at the end would come
too late. Instead it is checked **when it starts** (event trigger
`pg_hydradb_refuse_concurrent_index_drop`). It's refused the same way, with the
index left untouched.

- **Finding the index:** PostgreSQL doesn't say at that point which index a
  drop names, so the name is read from the statement. A concurrent drop is
  always a single `DROP INDEX CONCURRENTLY [IF EXISTS] name [CASCADE |
  RESTRICT]`. The name is resolved using the session's `search_path`.
- **If the name can't be read** (for example, a comment inside the statement),
  the drop goes through. The graph is then paused at the end instead, and its
  queries fail until a unique index is back and `refresh_graph` runs.

**A concurrent drop cancelled halfway** is not handled. That means one that got
past the check and then stopped after its first steps, because it was cancelled
or its session ended while waiting for other transactions. The index stays,
marked `INVALID`, enforcing nothing, and no event fires, so the graph isn't
paused. Until the index is dealt with:

- a write that adds an edge key the graph already has fails with
  `duplicate key value violates unique constraint "graph_adjacency_pkey"`,
  even though the table accepts it. A repeated vertex key is counted, and a
  query that reaches it fails;
- every `DROP INDEX` or `ALTER TABLE … DROP CONSTRAINT` in the database is
  refused with this key's `cannot drop the last … unique index` message;
- `refresh_graph` refuses, since an invalid index doesn't count.

To recover:
1. run the same `DROP INDEX CONCURRENTLY` again, which finishes it and pauses
   the graph;
2. add a unique index on the key;
3. run `refresh_graph`.

Declaring the key as a `PRIMARY KEY` or `UNIQUE` constraint avoids all this:
`DROP INDEX CONCURRENTLY` can't drop an index that backs a constraint.

**When these checks apply.** Only while the table has the graph's triggers.
That lets a `pg_dump --clean` restore, which drops those triggers first, then
drop the tables and constraints.

### Changes that pause the graph

A change the graph can recover from is allowed. It pauses the graph by setting
`hydradb.graphs.needs_refresh`, with a warning. Queries fail with the reason, as
for a long key, until `hydradb.refresh_graph` rebuilds the copy:

- **Rewriting column values.** An `ALTER TABLE` that rewrites values, such as
  `ALTER COLUMN … TYPE … USING …`, changes them without firing triggers (event
  trigger `pg_hydradb_flag_column_rewrite`).
  - PostgreSQL doesn't say which columns a rewrite changed, so rewriting any
    column of the table pauses the graph, and the refresh rebuilds it from the
    new values.
  - Rewrites that keep the values (adding a column with a default, changing
    persistence or access method) pause nothing, and neither do type changes
    that need no rewrite.
  - A rewrite that changes a graph column's type is refused instead (see
    above). One that keeps its type, such as `USING upper(kind)`, pauses the
    graph.
- **Dropping an edge end's last qualifying foreign key.** This isn't refused,
  because `pg_dump --clean` drops foreign keys before the triggers. While
  paused, the graph's triggers do nothing, so writes can't deadlock on them.
  `refresh_graph` refuses until a foreign key is back.
- **A pg_hydradb trigger that stops firing on every write:** `DISABLE TRIGGER`,
  `ENABLE TRIGGER` or `ENABLE REPLICA TRIGGER` on one that fired always. Writes
  made in the meantime aren't copied, so the graph stays paused even after the
  change is undone. This is how a data-only restore with `--disable-triggers`
  loads a table.
- **Dropping one of a graph's triggers.** This isn't refused, because
  `pg_dump --clean` drops the triggers first. The graph stays paused even if
  the trigger is created again by hand, since writes made without it aren't in
  the copy.

`refresh_graph` also puts the graph's triggers back as `create_graph` made
them: it creates any that were dropped and sets all of them to fire on every
write. So it doesn't matter whether you re-enable a trigger before or after
refreshing.

### When the checks are skipped

A superuser can turn event triggers off, and single-user mode runs none, so a
change can slip past these checks. Two things still catch it:

- **`refresh_graph`** compares every graph column with what was recorded at
  registration and refuses until the change is undone. It also checks every key
  and edge end's type and collation again.
- **Queries** check again that each table is still fit and still has its
  triggers, and refuse a key with a case-insensitive collation.

Writes made while a trigger was off are not in the copy.
`hydradb.refresh_graph(graph)` rebuilds it from the tables, blocking writes
until its transaction ends.

## Locking, isolation and retries

**Locking.** `create_graph`, `refresh_graph` and `drop_graph` lock all their
tables at once (`hydradb.lock_sources`):

- each attempt asks for every lock without waiting (`NOWAIT`);
- if any table is busy, it gives all of them back and tries again every 50 ms;
- it never holds one table while waiting for another, so a transaction that
  writes the tables in a different order neither deadlocks with it nor queues
  behind it;
- after `lock_timeout` (a minute when that is 0) it fails with
  `could not lock graph source …`. Retry once the writes finish.

**Isolation.** `create_graph`, `refresh_graph` and `drop_graph` must run in a
`READ COMMITTED` transaction, and refuse otherwise. They lock the tables and
then fill the copy from what they read, or check who owns the tables. A
`REPEATABLE READ` or `SERIALIZABLE` snapshot taken before the locks would miss
rows committed, or an owner changed, while they waited.

**Retries (SQLSTATE `40001`):**

- A writer whose `REPEATABLE READ` snapshot is older than a graph's
  registration, or older than a refresh that replaced the copy, gets `40001` and
  must retry its transaction. Otherwise its trigger would update rows it can't
  see. A refresh gives the graph a new registration number for this purpose.
- `drop_graph` and `refresh_graph` fail with `40001` if the graph was dropped,
  registered again or refreshed while they waited for its tables.

## Permissions and row-level security

**The copy is private.** Only the extension's functions read and write it. Every
call:

1. checks, as the caller, `SELECT` on every source table and `USAGE` on their
   schemas;
2. only then reads the copy as its owner, the way a `SECURITY DEFINER`
   function would.

So revoking a privilege stops graph queries at once.

**Row-level security.** A query never reads your tables' rows, so no
row-level security policy runs during it. A table with row-level security can
only be queried through the graph by a caller the security doesn't apply to:

- a superuser;
- a `BYPASSRLS` role;
- the table's owner, unless the table has `FORCE ROW LEVEL SECURITY`.

This is checked on every call. Any other caller is refused before any policy
runs. The copy holds every row, so a walk could otherwise pass through rows the
policies hide. Filtering only the final results is not enough for the same
reason: hidden vertices could still connect visible ones.

**Search path.** The extension's functions fix their `search_path` to trusted
catalog lookup. Temporary objects with catalog names, or a look-alike
privilege-check function, can't replace the extension's own checks.

## Removing a graph

`hydradb.drop_graph(graph)` removes a graph:

1. it drops the triggers `create_graph` installed;
2. it deletes the graph's registration and its rows in the copy.

Your tables and their data stay as they are.

**Don't delete from `hydradb.graphs` directly.** That leaves the triggers
behind, and they then fail every write to the tables with SQLSTATE `40001`.
`drop_graph` finds triggers by name and by the function they run, so it can
also clean up a graph removed that way. It deletes every row in the copy that no
registered graph owns.

**Triggers that aren't pg_hydradb's are left alone.** `create_graph` refuses to
register over a trigger of the same name that runs something else, and
`drop_graph` leaves such a trigger in place.

A graph registered again under the same name gets a new registration number,
which its rows in the copy are keyed by.

## Backups, restore and replication

**`pg_dump`** includes graph registrations and the copy. The rows of
`hydradb.graphs`, `graph_vertex_tables`, `graph_edge_tables`, `graph_vertices`
and `graph_adjacency`, and their sequences, are dumped with your tables and
their triggers. A restore loads table data before it creates triggers, so the
copy comes from the dump, not from the triggers.

**A dump of only your schemas** (`pg_dump -n app`) leaves out the extension but
keeps the triggers on your tables. Restore it into a database that already has
`CREATE EXTENSION pg_hydradb`, and register the graphs again there before
writing to the tables.

**A schema-only dump** (`pg_dump --schema-only`, also the usual first step for a
logical replication subscriber) restores the triggers but not the registrations,
which are data.

- **Every write fails until then.** Until the graphs are registered again,
  writes to their tables fail with `graph registration is not visible; retry
  the source transaction` (SQLSTATE `40001`), and retrying doesn't help.
- **That includes replication.** The triggers are `ENABLE ALWAYS`, so changes
  applied by a subscription fail too. Its apply worker then retries the same
  transaction forever, while the publisher keeps its WAL.
- **Before any data reaches the restored tables,** either run `create_graph`
  for each graph (which replaces the restored triggers), or run
  `hydradb.drop_graph(name)` to remove them if the target shouldn't have a
  graph.

To list the graphs whose triggers were restored:

```sql
SELECT DISTINCT convert_from(substring(tgargs FROM 1 FOR position('\x00'::bytea IN tgargs) - 1), 'UTF8') AS graph
FROM pg_trigger
WHERE tgfoid IN ('hydradb.maintain_rows()'::regprocedure);
```

## Upgrading

This preview is unreleased, and its `0.1.0` script still changes between builds,
so there's no `ALTER EXTENSION pg_hydradb UPDATE`. A database installed from an
earlier build has to drop the extension and create it again.

**Never use `DROP EXTENSION pg_hydradb CASCADE` for this.** It also drops,
without asking, any view, function or other object of yours that uses the
extension. Instead:

```sql
SELECT hydradb.drop_graph(name) FROM hydradb.graphs;  -- builds that have drop_graph
DROP EXTENSION pg_hydradb;                            -- no CASCADE
CREATE EXTENSION pg_hydradb;                          -- then create_graph each graph again
```

Without `CASCADE`, `DROP EXTENSION` refuses and lists every object that still
depends on the extension (long lists are finished in the server log). Save
your own objects from that list and recreate them afterwards.

A build without `drop_graph` lists its graphs' `pg_hydradb_…` triggers there
too. When those are the only objects listed, `CASCADE` removes just them and is
safe.

## Graph crates

- **`hydradb-pg-graph-core`** has the `Vertex`, `Edge` and `GraphSnapshot` types
  and the `GraphRead` trait.
  - A walk reads edges only through `GraphRead::expand` (`&mut self`). That lets
    a graph that loads on demand fetch a whole layer in one batch. It returns an
    `Expansion` that reads that layer (`&self`).
  - A vertex whose edges weren't loaded is an error, not a panic.
- **`hydradb-pg-graph-query`** has the walks, which work over any `GraphRead`,
  and the path search, which works over a `CsrGraph`.
- **`hydradb-pg-graph-csr`** is that in-memory graph.

This extension implements `GraphRead` over the copy. The path search compares
vertex ids, so it runs on an in-memory graph of just the region it read,
numbered by `(label, key)`.

To add a new storage, implement `GraphRead`. SPI, permissions, triggers and
transaction-id handling stay in this adapter.

## Optional benchmarks

```sh
PGHYDRADB_QUERY_BENCHMARK=1 PGHYDRADB_QUERY_BENCHMARK_ITERATIONS=7 just pg-test
```

This uses only the throwaway test server. It builds two fixed graphs, each with
10,000 vertices and 60,000 edges:

- a one-way cycle with six separate parallel edges per hop;
- a broader, hub-heavy graph: a cycle plus 50,000 edges whose sources are skewed
  (a cubic skew) and whose targets are spread by modular arithmetic.

It runs reachable vertices, reachable edges, and one path between a pair, at
depths 5, 8 and 10. For each query it prints JSON lines with p50 and p95
(nearest rank), the response size, and the full last response.

- **`cold_backend`** opens a new connection for each sample.
- **`repeated_backend`** warms up one connection and reuses it.

"Cold" here doesn't mean PostgreSQL's shared buffers or the OS page cache are
empty. Timings include the SQL round trip and decoding the full JSON, not
opening the connection or setting up the graph.

**Limits of these numbers:**
- the cycle has tiny neighbourhoods;
- the skewed graph does reach more vertices, but it isn't a fitted power-law
  workload;
- neither is a performance promise for any application. Measure with your own
  graph shape, row-level security, concurrent clients and hardware before
  setting a target.

```sh
PGHYDRADB_WRITE_BENCHMARK=1 just pg-test
```

This builds a graph of 10,000 pages and 100,000 links (`--vertices` and
`--edges` in `tests/write_benchmark.py` change the size). It reports:
- how long `create_graph` takes to fill the copy;
- the size of the copy's tables;
- for 1, 100 and 10,000 edges, the time to UPDATE, then INSERT, then DELETE
  that many edges with their triggers, and for the next one-hop query after
  each.

One run with 100,000 pages and 1,000,000 links, with foreign keys from both
link columns to the pages (PostgreSQL 18, `shared_buffers = 4GB`, an AWS
Graviton server with 8 vCPUs and 64 GB of RAM). Each figure is one statement,
the last of three runs:

| Write | 1 edge | 100 edges | 10,000 edges |
|---|---:|---:|---:|
| UPDATE (move each edge's target) | 2.6 ms | 9.9 ms | 0.47 s |
| INSERT new edges | 1.5 ms | 5.6 ms | 0.35 s |
| DELETE edges | 1.2 ms | 3.2 ms | 0.12 s |

- **Fill:** `create_graph` took 18 s. The copy used 297 MB for the adjacency
  table and 16 MB for the vertex table.
- **First runs are slower:** the first two 10,000-edge UPDATEs after the fill
  took 2.0 s and 1.3 s.
- **Less memory:** with the default `shared_buffers = 128MB`, the 10,000-edge
  batches took 0.76 s (UPDATE), 0.40 s (INSERT) and 0.22 s (DELETE).
- **Reads stay fast:** the one-hop query after each write took 0.3–1.1 ms.

These numbers depend on hardware, memory and graph shape. Run the benchmark on
your own server before relying on them.
