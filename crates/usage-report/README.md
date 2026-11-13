# Anonymous usage report

GraphNode can send a small, anonymous report about how the install is used, so
HydraDB can see how many installs there are, whether they keep running, and
what stops them. It is **on by default**. Set
`GRAPH_USAGE_REPORT_ENABLED=false` to turn it off.

This crate holds what the sender and the receiver agree on: the install ID, the
report format, how running totals become "since the last report", and the
receiver's check. GraphNode's sender is `src/bin/graph_node/usage_report.rs`;
the receiver route is `POST /v1/usage` on `query-telemetry-receiver`.

## Turn it on or off

| Variable | Default | Meaning |
| --- | --- | --- |
| `GRAPH_USAGE_REPORT_ENABLED` | `true` | Send reports |
| `GRAPH_USAGE_REPORT_URL` | `https://telemetry.hydradb.com/v1/usage` | Where to send them |
| `GRAPH_USAGE_REPORT_INTERVAL_MS` | `3600000` (1 hour) | How often; at least `60000` |

The node logs its install ID when reporting is on and an ID is available, along
with how to turn reporting off. When disabled, it starts no reporter, reads or
writes no install ID, and sends nothing. Changes need a restart.

## What a report contains

Every counter is since the last delivered report of the same process.

| Part | Contents |
| --- | --- |
| Identity | `install_id`, a random `run_id` per process, a random `report_id` kept until that report is delivered |
| Build | Version and commit, OS, CPU architecture, object store type (`aws`, `local`), default Cypher engine |
| Cluster | Live node count, configured cell count |
| Queries | Started, completed, failed, rows returned, cancellations, auth failures, scope denials, backpressure waits, prepares |
| Failures | Counts by error class, and by engine, stage and failure reason |
| Latency | Read and write latency histograms per engine |
| Operators | Experimental-engine totals per physical operator name: requests, invocations, rows, time, storage time and requests, estimate misses |
| First query | How long after start the process's first query succeeded, and its first failed |

## What it never contains

Query text or hashes of it, parameters, labels, relationship types, property
names or values, namespace, graph, scope, cell, node or pod names, IP
addresses, credentials, or anything else the customer named. Scope and cell
names key operator totals inside the node so counts from different scopes stay
separate; they are dropped before the report is built, and a test checks that
they never reach it. The receiver turns away any report with anything but a
short identifier where a name goes.

## The install ID

A random 128-bit ID, stored as 32 lowercase hex characters at
`<GRAPH_DATA_PATH>/_usage_report/v1/install_id` in the object store. The first
node creates it with a create-only write. Concurrent nodes read the winning ID.
It survives node replacement. A copy of the bucket carries the same ID.

A read-only bucket without an existing ID cannot get one, so its nodes send
nothing. Read or write errors also disable reporting on that node. An invalid
stored ID is left untouched and the node sends nothing. The in-memory store
has no lasting ID and sends nothing. ID resolution runs in the reporter task
under one 30-second deadline, so store retries do not delay node startup. If
resolution fails or times out, that task exits without sending any reports,
including at shutdown.

## Delivery

The first report is sent after 10 minutes, or one interval if that is shorter.
Later reports use the configured interval with up to 10% jitter.

A background task copies the node's existing counters, subtracts what it last
sent, and POSTs one report: 5 s timeout, up to 3 tries on a network error, `429`
or `5xx`. Queries never wait on it and nothing is queued. A report that fails is
dropped, and the next one is measured from the last delivered report, so its
counts are not lost. The next one also keeps the failed report's `report_id`. If
the receiver did get the failed report and only its reply was lost, the new one
replaces it rather than counting it twice. Each report is stamped at least a
millisecond after the one before it, even if the clock steps back, and the
receiver keeps the copy with the later stamp. So the failed report cannot
replace the new one by arriving after it. A stop during a send works the same
way. A clean shutdown sends what is left within one 2 s budget for metrics
collection and the POST. A crash loses any counts not yet delivered.

## Known gaps

The `charts/hydradb` NetworkPolicy allows HTTPS egress only to the peers in
`networkPolicy.httpsEgressTo`, so a chart install delivers reports only once
the report host is allowed there or in `networkPolicy.additionalEgress`. Until
then the node logs one failed delivery and carries on.

Operator totals undercount work done in a scope evicted between two reports.
This occurs only on nodes at `max_open_scopes`; Prometheus `/metrics` behaves
the same way. It also covers a report re-sent after a lost reply: if a scope
was evicted in between, or the shards missed their time budget, the re-send
replaces the first copy without that scope's operator counts. Query, failure
and latency counts are not affected. Graph size, including node and relationship counts, is not
reported yet.

## Verify

```sh
just test-usage-report
just test-usage-report-node
```
