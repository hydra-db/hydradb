# pg_hydradb

Graph queries inside PostgreSQL, over tables you already have.

You tell pg_hydradb which tables hold your **vertices** (things, like people or
pages) and which hold your **edges** (links between them). It keeps a compact
copy of those links up to date as you write, and answers questions like
"what's within 3 hops of this page?" or "how is A connected to B?" in one SQL
call, returning JSON. Your data stays in your tables; PostgreSQL still handles
transactions, permissions, backups and recovery.

Works with PostgreSQL 16, 17 and 18.

## Install

From the repository root, with your PostgreSQL's `pg_config` on `PATH`:

```sh
just pg-install 18      # or 16, 17
```

Add `pg_hydradb` to the `shared_preload_libraries` setting in
`postgresql.conf`, so the server loads it at startup. Queries work without it,
but the usage report (below) needs it.

```
shared_preload_libraries = 'pg_hydradb'
```

If the setting already lists other libraries, add `pg_hydradb` to the list
rather than replacing them, separated by a comma:

```
shared_preload_libraries = 'pg_stat_statements,pg_hydradb'
```

Instead of editing the file, a superuser can run
`ALTER SYSTEM SET shared_preload_libraries = 'pg_hydradb';` (with the existing
libraries in the list, as above). Either way, restart PostgreSQL afterwards: a
reload is not enough. `SHOW shared_preload_libraries;` then includes
`pg_hydradb`.

Then, in your database:

```sql
CREATE EXTENSION pg_hydradb;
```

To run the test suite against a throwaway server: `just pg-test`.

## Register a graph

```sql
SELECT hydradb.create_graph('social',
  '[{"label": "person", "table": "public.people", "key": "id"}]',
  '[{"label": "knows", "table": "public.friendships", "key": "id",
     "source": "person_a", "source_label": "person",
     "target": "person_b", "target_label": "person"}]');
```

A graph can span many vertex and edge tables. You can also name a `sort_key`
column (which vertices to keep first when a result is cut short) and a `kind`
column (an edge's type, for filtering).

## Query it

| Function | Answers |
|---|---|
| `hydradb.reachable_vertices(graph, seeds, options)` | Which vertices are within N hops, and how far |
| `hydradb.reachable_edges(graph, seeds, options)` | Which edges are within N hops |
| `hydradb.paths_between(graph, sources, targets, options)` | Shortest paths between vertices |
| `hydradb.paths_from(graph, sources, options)` | Paths from vertices to anywhere |

```sql
SELECT hydradb.reachable_vertices('social', '{"person": [1]}', '{"max_depth": 3}');
SELECT hydradb.paths_between('social', '{"person": 1}', '{"person": 42}', '{}');
```

Seeds name vertices by table label and key: `{"person": 1}` or
`{"person": [1, 2]}`. Results come back the same way, so join them to your
tables for names and properties. Common options: `max_depth`, `direction`
(`outgoing`, `incoming`, `both`), `edge_kind` / `edge_kinds`, and
`paths_per_pair`.

## Requirements

pg_hydradb needs ordinary tables with integer, text or uuid keys, NOT NULL
columns, a primary key or unique constraint on each key, and a foreign key from
each edge column to its vertex table.

`create_graph` will tell you if a table needs a change first, and the error's
hint gives you the exact `ALTER TABLE` to run. The usual ones are a nullable
column, a key without a unique index, an edge column without a foreign key
(without one, concurrent writers can deadlock), or a table type pg_hydradb can't
keep in sync (views, partitioned tables, inheritance).

## Schema changes

**Some changes pause a graph until you refresh it.** When a change means the
graph's copy might be out of date, queries return an error saying what
happened instead of a possibly stale answer. Run
`SELECT hydradb.refresh_graph('social');` to rebuild it. This happens after:

- rewriting a column's values (`ALTER COLUMN … TYPE … USING …`);
- disabling or dropping one of the graph's triggers (the refresh puts it
  back);
- dropping an edge column's foreign key (add it back first);
- writing a text key longer than 2,000 bytes (shorten or remove it first).

**Changes to what the graph uses are refused.** You can add columns, indexes
and constraints, and change columns the graph doesn't read. But renaming,
retyping or dropping a column the graph reads, making it nullable, dropping the
table or its key's last unique index, or adding the table to an inheritance tree
is refused. Drop the graph first (`hydradb.drop_graph('social')`), make the
change, and register it again.

Other triggers on your tables are fine, including ones that write back to the
same table.

## Row-level security

A role that row-level security applies to gets an error, not a filtered
answer, because pg_hydradb would otherwise walk through rows that role can't
see. Query as the table owner or a role that bypasses RLS.

## Tips

- Run `create_graph`, `refresh_graph` and `drop_graph` in `READ COMMITTED`
  transactions (the default).
- In `REPEATABLE READ` transactions, a write right after a `refresh_graph` may
  ask you to retry (`40001`), as with any serialization conflict.
- Upgrading: `drop_graph` each graph, `DROP EXTENSION pg_hydradb` (never
  `CASCADE`), install the new version, `CREATE EXTENSION`, then register
  again.

## Usage reports

pg_hydradb sends HydraDB one small anonymous report an hour, so we learn how many
installs there are, whether they use it, and what gets in their way. It is on
by default. To turn it off, set this in `postgresql.conf` and reload:

```
pg_hydradb.usage_report = off
```

At startup the server logs that reporting is on and how to turn it off.

**What is sent:** counts, ranges, timestamps and codes from fixed lists. Every
field is listed in `crates/pg-usage-report/src/lib.rs`.

- An install ID: a hash of the cluster's system identifier. It stays the same
  across restarts. A `pg_dump` restore gets a new one.
- The extension version, PostgreSQL major version, OS and CPU architecture.
- For each database with pg_hydradb: the number of vertex and edge tables in
  each graph, whether the graph needs a refresh, and the row counts and size of
  the adjacency tables, rounded to a power of ten.
- Since the server started: calls to each function, their latency, the rows
  they read and the hops they reached, as histograms; results cut short, by
  reason; and failures and refusals, by reason (such as `needs_refresh`,
  `nullable_column` or `no_unique_index`).

**What is never sent:** database, schema, table, column, graph or label names,
keys, seeds, options, query results, or anything else you typed.

Run `SELECT hydradb.usage_report();` to see the report for the current
database. The background worker sends one report covering every database. On a
server with no internet access, nothing reaches us. You can send us that
output yourself if you want to.

Without `shared_preload_libraries`, nothing is sent, and `usage_report()`
returns no activity. The counters need shared memory, because a refusal
rolls back its transaction, and a counter kept in a table would roll back with
it.

| Setting | Default | |
| -- | -- | -- |
| `pg_hydradb.usage_report` | `on` | Turning it on again needs a restart |
| `pg_hydradb.usage_report_url` | `https://telemetry.hydradb.com/v1/pg-usage` | |
| `pg_hydradb.usage_report_interval` | `1h` | The first report is sent 10 minutes after startup |

## More detail

Every rule, option, limit and edge case is in [REFERENCE.md](REFERENCE.md).
