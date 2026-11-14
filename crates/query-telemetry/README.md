# Failed-query telemetry

GraphNode reports failed queries to an independent Axum receiver. The receiver
buffers reports and batch inserts them into ClickHouse. MCP uses GraphNode's
HTTP query API, so reporting also works for MCP without a second exporter.
Bolt and direct client-service queries use the same reporting path.

## Run the receiver

Create a ClickHouse database first, then start the receiver:

```sh
export CLICKHOUSE_URL=http://127.0.0.1:8123
export CLICKHOUSE_DATABASE=default
export QUERY_TELEMETRY_ADDR=127.0.0.1:18500
just query-telemetry-receiver
```

The receiver creates `query_failures`, `usage_reports` and `pg_usage_reports` in the selected
database at startup, so its user needs `CREATE TABLE` and `INSERT` there.
`CLICKHOUSE_USER` defaults to `default`. Set `CLICKHOUSE_PASSWORD_FILE` to a
password file when authentication requires it. For ClickHouse Cloud, use its
HTTPS endpoint. Set `QUERY_TELEMETRY_TOKEN_FILE` to a bearer-token file to
authenticate incoming reports. Configure the same token file on GraphNode.

On a replicated ClickHouse cluster (for example, several replicas behind one
load-balanced endpoint), create the database `ON CLUSTER` and set
`CLICKHOUSE_CLUSTER` to the cluster's name:

```sql
CREATE DATABASE IF NOT EXISTS telemetry ON CLUSTER <cluster_name>;
```

The receiver then creates both tables `ON CLUSTER`, with
`ReplicatedReplacingMergeTree`, so they exist on every replica and a row
inserted through any replica reaches all of them. Without it, each table is a
local table on whichever replica took the statement, and inserts and reads that
reach another replica fail with `UNKNOWN_TABLE`. An `ON CLUSTER` statement
waits for every replica, so the receiver cannot start while one is down; a
running receiver keeps inserting through the replicas that are up.

| Receiver variable | Default | Meaning |
| --- | --- | --- |
| `QUERY_TELEMETRY_ADDR` | `127.0.0.1:18500` | HTTP listen address |
| `QUERY_TELEMETRY_TOKEN_FILE` | unset | Optional authentication for failed-query reports |
| `QUERY_TELEMETRY_BUFFER_EVENTS` | `1024` | Maximum queued events per route, up to 16384 |
| `QUERY_TELEMETRY_BATCH_EVENTS` | `100` | Maximum batch size, up to 1024 |
| `QUERY_TELEMETRY_FLUSH_MS` | `1000` | Flush interval, up to 60000 ms |
| `CLICKHOUSE_URL` | `http://127.0.0.1:8123` | ClickHouse HTTP endpoint |
| `CLICKHOUSE_DATABASE` | `default` | Existing database |
| `CLICKHOUSE_CLUSTER` | unset | Cluster name of a replicated ClickHouse; tables are created on every replica |
| `CLICKHOUSE_USER` | `default` | ClickHouse user |
| `CLICKHOUSE_PASSWORD_FILE` | unset | ClickHouse password file |

`POST /v1/query-failures` accepts one version-1 JSON report and returns `202`
once it enters the memory buffer. A full queue returns `429`. Invalid reports
return `422`; invalid authentication returns `401`. Bodies are limited to 128 KiB.
`GET /healthz` reports queue size, pending events including the current batch,
inserted event count and ClickHouse health.
`GET /readyz` returns `503` after an insert failure or while the queue is full.

`POST /v1/usage` accepts one anonymous usage report (see
[`crates/usage-report`](../usage-report/README.md)) and answers the same way,
with two differences. It takes **no token**, even when
`QUERY_TELEMETRY_TOKEN_FILE` is set: every install sends to it, and a token in a
released binary cannot stay secret. Instead, bodies are limited to 16 KiB, and
a report must pass `hydradb_usage_report::validate`, which pins the version and
ID shapes, bounds every collection, and accepts only short identifiers where
names go. The stored `payload` is the report re-serialized from its type, so
fields this receiver does not know are dropped rather than stored. The route
does not rate-limit; when it is exposed publicly, put a per-IP limit at the
ingress in front of it.

`POST /v1/pg-usage` accepts pg_hydradb's hourly report (see
[`crates/pg-usage-report`](../pg-usage-report/src/lib.rs)) on the same terms:
no token, 16 KiB, and `hydradb_pg_usage_report::validate`. It is stored in
`pg_usage_reports`. Its activity counts are totals since the PostgreSQL server
started, so take the latest row per `(install_id, server_started_at_unix_ms)`
and sum those. Each route accepts only its own report format.

## Enable or disable GraphNode reporting

Reporting requires the optional `query-telemetry` Cargo feature and is disabled at runtime by default.
To enable it for a local MCP stack:

```sh
export GRAPH_QUERY_TELEMETRY_ENABLED=true
export GRAPH_QUERY_TELEMETRY_URL=http://127.0.0.1:18500/v1/query-failures
# If the receiver requires authentication:
# export GRAPH_QUERY_TELEMETRY_TOKEN_FILE=/path/to/telemetry-token
just mcp-local-query-telemetry /tmp/hydradb-mcp
```

These variables belong to GraphNode. The MCP local launcher passes them through
to its child node. For an existing node, set them in that node's environment.
For an MCP client configuration, put them in the launcher's `env` map.

To turn reporting off, restart GraphNode with:

```sh
export GRAPH_QUERY_TELEMETRY_ENABLED=false
```

When disabled, GraphNode starts no exporter and does not read its telemetry URL
or token file. Changes to these variables require a restart.

Reports contain the query ID and text, SHA-256 of the full text, graph scope,
cell, node, protocol, server version, configured and selected Cypher engine,
prepare/execute failure stage, stage duration, runtime limit, consistency,
requested read epoch, pagination metadata, error class, reason and limit operation.
Parameter counts and up to 32 parameter names are included. Parameter values,
auth credentials, bookmark strings and cursor tokens are omitted.

Enabling reporting exports raw query text, which can contain literal data.
Query text is limited to 16 KiB with a truncation flag. The `error` field contains
the error class only. Arbitrary error messages can contain bound values and are never exported.
`elapsed_us` covers the failing service stage, not the entire client
workflow. A failure during a later page keeps that page's continuation flag.
Each failing stage emits one event; successful queries emit none.

HTTP callers can send `X-HydraDB-Correlation-Id` and `X-HydraDB-Caller-Step`.
Bolt callers use the existing `hydradb.correlation_id` and `hydradb.caller.step`
transaction metadata. MCP labels requests `mcp.execute_query`.
Connection failures, malformed HTTP/Bolt messages, authentication failures,
MCP argument validation and client disconnections before a service error are
outside the query-reporting boundary.

## Buffering and retries

GraphNode uses one background worker, a 256-event queue, and an 8 MiB retained-payload budget shared with in-flight delivery.
Admission reserves both a queue slot and payload bytes before copying report fields.
Successful queries
take a timing snapshot without hashing, truncating, or copying query strings.
On failure, the service copies reportable fields and queues a deferred report.
The exporter hashes the query and formats the event. Query execution
never waits for HTTP delivery or queue space. The exporter makes up to three
attempts per event, with a two-second timeout per attempt. Network errors, HTTP
429 and HTTP 5xx are retried. Queue overflow and exhausted retries drop the
event and log a warning without its query or credentials. Node shutdown allows
five seconds for pending reports to drain.

The receiver flushes on batch size or interval. A failed ClickHouse insert keeps
the batch and retries with backoff, up to ten seconds between attempts. Further
events fill the queue, then receive `429`. SIGINT and SIGTERM stop HTTP intake.
HTTP requests and the final batch flush share a thirty-second shutdown deadline.
An unfinished request body cannot keep the receiver alive past this deadline.
A failed drain exits with an error.

Both queues are bounded and live in memory. `202` acknowledges buffering, not a
durable ClickHouse write. Crashes and exhausted shutdown deadlines can lose
pending events. Retries can create duplicate rows. Events and usage reports
retain their IDs across retries, and both tables use `ReplacingMergeTree`; query
with `FINAL` when you need immediate deduplication. Of two `usage_reports` or `pg_usage_reports` rows
with one `report_id`, the one with the later `sent_at_unix_ms` stays, whichever
arrived last.

ClickHouse inserts use its [HTTP API](https://clickhouse.com/docs/interfaces/http)
with `JSONEachRow` and `wait_end_of_query=1`. The receiver checks both status and
the response body before removing a batch from memory.

## Query the reports

Common dimensions have dedicated columns. `event` holds the full versioned
report as a JSON string, including error details and caller metadata.

```sql
SELECT engine, stage, error_class, failure_reason, count()
FROM query_failures FINAL
GROUP BY engine, stage, error_class, failure_reason
ORDER BY count() DESC;

SELECT query, JSONExtractString(event, 'error') AS error,
       JSONExtractString(event, 'caller_step') AS caller_step
FROM query_failures FINAL
ORDER BY timestamp_unix_ms DESC
LIMIT 20;
```

## Verify

`just clippy-query-telemetry` checks the standalone crate. To verify real MCP
failures, batches, insert retry, authentication, shutdown, the node's own usage
report and disabled reporting against a disposable ClickHouse database:

```sh
just build-mcp-query-telemetry build-query-telemetry
CLICKHOUSE_URL=http://127.0.0.1:8123 just smoke-query-telemetry
```

The smoke script creates and removes its own database. It requires permission
to create databases and uses `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD_FILE`.
With `CLICKHOUSE_CLUSTER` set, it runs its own DDL `ON CLUSTER` and the receiver
creates replicated tables.
It does not run `cargo test`.
