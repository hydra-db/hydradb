---
title: "RFC 0008: HTTP Service, Reader/Writer Fleet & Error Taxonomy"
status: draft
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0001-strong-consistency-model.md
  - 0004-graph-data-model-and-write-path.md
  - 0006-index-framework.md
  - 0007-opencypher-read-path.md
  - 0017-observability-and-metrics.md
---

# RFC 0008: HTTP Service, Reader/Writer Fleet & Error Taxonomy

## Summary

This RFC decides the network edge of turbolay: the HTTP service, how the same binary deploys as either a **writer** or a **reader** node, the two API surfaces (data plane + admin plane), the wire-level error taxonomy, and the fleet/deployment shape that makes reader/writer node separation (goal #2, D9) real.

It is deliberately thin. The hard problems — consistency (RFC 0001), the write fan-out (RFC 0004), indexes (RFC 0006), the Cypher read path (RFC 0007) — are decided elsewhere; this RFC only exposes them over HTTP and names the failure modes. The service is an `axum::Router` built on the opendata `server/` shape already proven by the sister `log` and `vector` crates: `AppState { backend, metrics }`, read routes always registered, write/admin routes registered only for the writer backend, `.layer(Tracing).layer(Metrics)`, `axum::serve` with graceful shutdown on SIGTERM/SIGINT, `/-/healthy` / `/-/ready` / `/metrics`, and an `into_router()` seam so tests drive routes via `oneshot()` without binding a socket.

The structural fact everything rests on (D2/D13): **one writer per namespace, many readers.** The writer fronts a fenced `Db`; readers front unfenced, manifest-polling `DbReader`s (RFC 0001) and scale horizontally behind a load balancer. Same binary, `--role {writer,reader}`.

## Decision

- **One binary, two roles.** `turbolay --role writer` registers data-plane write routes + the admin plane and opens a fenced `Db`. `turbolay --role reader` registers read/query routes only and opens a `DbReader`; write and admin routes are **not registered**, so they 404 (mirrors opendata's read-only gateway).
- **One namespace per process** (Q19/D8): a node serves exactly one namespace = one SlateDB DB = one S3 prefix = one tenant = one graph. Fleet horizontal scale is per-namespace reader replicas; multi-namespace hosting is a lazy-open optimization deferred past POC scale (open-decisions Q19).
- **Two surfaces**: a **data plane** (`/v1/upsert`, `/v1/delete`, `/v1/query`) and an **admin plane** (`/v1/admin/*`: index create/drop/status, namespace lifecycle).
- **Session-token consistency on the wire** (Q11/Q12, RFC 0001): write responses return `{ ok, seq }`; query requests carry `{ consistency: { session: <seq> } | { strict: true } }`; every query response returns `latest_seq`. The reader gates on the token before serving.
- **A typed error taxonomy** with stable `error` codes, an HTTP status, and a `retryable` boolean, so a load balancer or client can act on `reader_behind` mechanically.
- **Auth posture v0 = trusted network.** No authn/authz is built. The service assumes a private network / service mesh boundary; a token/mTLS layer is a later concern and is noted, not designed, here.

## Data plane

All data-plane routes are `POST` under `/v1`, JSON in and JSON out. Write routes (`/v1/upsert`, `/v1/delete`) exist only on the **writer**; `/v1/query` exists on both roles but is the reader's whole job.

### POST /v1/upsert — nodes + edges batch (RFC 0004)

One request is a batch of `UpsertNode` / `UpsertEdge` operations lowered by the writer into a single atomic `WriteBatch` (RFC 0004 write path). Nodes and edges address each other by `xid`; endpoints are created implicitly if absent (`resolve_or_create`).

```json
{
  "nodes": [
    { "xid": "chunk:42", "labels": ["Chunk"],
      "props": { "text": "rust is memory-safe", "ord": 3 } },
    { "xid": "ent:rust", "labels": ["Entity"],
      "props": { "name": "rust", "kind": "language" } }
  ],
  "edges": [
    { "src": "chunk:42", "pred": "MENTIONS", "dst": "ent:rust",
      "props": { "confidence": 0.91 } }
  ]
}
```

Response (RFC 0001 write ack):

```json
{ "ok": true, "seq": 105 }
```

`seq` is turbolay's logical sequence (== `m/latest_seq` == injected SlateDB `seqnum`), the session token the client carries into later reads. The whole batch commits at one seq or not at all; a partial batch is never observable (RFC 0004 acceptance §6).

### POST /v1/delete — nodes / edges, with `detach` (RFC 0004)

Deletes are tombstone-and-filter by default (O(1), degree-independent, Q8/D10). `detach: true` on a node delete requests explicit `DETACH DELETE`-style eager removal of incident edges.

```json
{
  "nodes": [ { "xid": "chunk:42", "detach": true } ],
  "edges": [ { "src": "chunk:42", "pred": "MENTIONS", "dst": "ent:rust" } ]
}
```

```json
{ "ok": true, "seq": 106 }
```

Deleting an absent `xid` is a no-op (not an error), consistent with RFC 0004's `DeleteNode`.

### POST /v1/query — openCypher read (RFC 0007)

The reader's primary surface. Body is the RFC 0007 request; `consistency` is the RFC 0001 object; `brute_force` opts into a full scan on unindexed predicates (Q15/RFC 0006); `debug` requests per-phase stats (RFC 0017).

```json
{
  "cypher": "MATCH (e:Entity {name:'rust'})<-[:MENTIONS]-(c:Chunk) RETURN c LIMIT 20",
  "params": { },
  "consistency": { "session": 105 },
  "brute_force": false,
  "debug": false
}
```

Response — results plus the freshness watermark the client keeps for read-your-writes:

```json
{
  "results": [ { "c": { "xid": "chunk:42", "labels": ["Chunk"],
                        "props": { "text": "rust is memory-safe", "ord": 3 } } } ],
  "latest_seq": 112
}
```

With `consistency.session = T`, the reader blocks in the freshness gate (`durable_seq >= T`, RFC 0001) before executing; with `strict: true` it advances to the freshest durable state it can reach (bounded by `manifest_poll_interval`); with neither it serves bounded-stale. `debug: true` adds a `stats` object (gate wait, anchor selectivity, per-hop frontier sizes, changelog-tail entries scanned — RFC 0017). BFS depth for variable-length paths is capped by `bfs_depth_cap` (Q17); exceeding it is a `bfs_depth_exceeded` error (below).

## Admin plane

Admin routes live under `/v1/admin` and exist **only on the writer** (index maintenance and namespace lifecycle are write operations under the single-writer invariant, RFC 0006). Readers 404 them.

### POST /v1/admin/index — create an index (RFC 0006)

Declares a value/label/count index; the writer registers it (`m/index/{id}`) and backfills existing data asynchronously (Q-new; RFC 0006 lifecycle `creating → backfilling → live`).

```json
{ "label": "Entity", "property": "name", "tokenizer": "exact" }
```

```json
{ "ok": true, "index_id": "idx_entity_name", "state": "backfilling", "seq": 107 }
```

`tokenizer` ∈ `{ exact, hash, int, float }` (Q14; DateTime → `int`/epoch). An unknown tokenizer or a tokenizer/type mismatch is a `400 invalid_index_spec`.

### DELETE /v1/admin/index/{id} — drop an index (RFC 0006)

```json
{ "ok": true, "index_id": "idx_entity_name", "state": "dropping", "seq": 108 }
```

Drop drains the index keyspace (range-delete under its prefix) and the planner stops consulting it; the response returns immediately with `state: "dropping"`.

### GET /v1/admin/index — list + build status (RFC 0006)

```json
{
  "indexes": [
    { "index_id": "idx_entity_name", "label": "Entity", "property": "name",
      "tokenizer": "exact", "state": "live", "watermark": 112, "latest_seq": 112 },
    { "index_id": "idx_chunk_ord", "label": "Chunk", "property": "ord",
      "tokenizer": "int", "state": "backfilling", "watermark": 96, "latest_seq": 112 }
  ]
}
```

`state ∈ { creating, backfilling, live, dropping }`; `watermark < latest_seq` means a backfill is in flight (reads stay correct via the changelog tail — RFC 0001/0006). This endpoint is how an operator watches a build complete.

### Namespace lifecycle

A namespace is one SlateDB DB per S3 prefix (Q19). **Creation is implicit on first write** (RFC 0004 `resolve_or_create` opens/creates the namespace's DB on the writer's first `/v1/upsert`); there is no mandatory pre-create step. Explicit endpoints exist for provisioning and teardown:

- `POST /v1/admin/namespace` — pre-create (idempotent; a no-op if the prefix already has a manifest). Body `{ "namespace": "tenant-a" }` → `{ "ok": true, "created": true }`.
- `GET /v1/admin/namespace` — list namespaces visible under the configured storage root → `{ "namespaces": ["tenant-a", "tenant-b"] }`.
- `DELETE /v1/admin/namespace/{ns}` — drop = delete the S3 prefix's DB (the free atomic drop Q19 buys). Returns `{ "ok": true }`; refuses with `409 namespace_in_use` if a live writer holds the epoch.

A write to (or query of) a namespace that does not exist and cannot be created — reader role querying a never-written prefix — returns `namespace_not_found` (below). On the writer, first write creates it, so `namespace_not_found` is a reader-side condition.

## Error taxonomy

Every error response is `{ "error": <code>, "retryable": <bool>, ... }` with a stable machine `error` code and an HTTP status. `retryable` tells a client/LB whether a mechanical retry can succeed. Codes:

| `error` | HTTP | retryable | Source | Meaning / caller action |
|---|---|---|---|---|
| `reader_behind` | 503 | **true** | RFC 0001 | Reader could not reach the session token within `gate_timeout` (reader-replay lag). LB/client retries another reader (likely caught up) or falls back to the writer. |
| `index_behind` | 503 | **true** | RFC 0007 | Changelog tail exceeded `tail_max_entries` and a used index's watermark did not advance within `tail_wait_timeout` (index-builder/backfill lag, one layer below `reader_behind`). Retry after backfill catches up. |
| `unindexed_property` | 400 | false | RFC 0006 | Filter on a property with no declared index. Declare an index or resend with `brute_force=true`. |
| `malformed_cypher` | 400 | false | RFC 0007 | Parse error / syntactically invalid Cypher. Body names the offending construct + position. |
| `unsupported_cypher` | 501 | false | RFC 0007 | Well-formed but outside the v0 read subset (e.g. aggregation, `WITH`, writes). Points to the full-Cypher backlog RFC 0013. |
| `oversize_node` | 413 | false | RFC 0004 | Encoded `NodeRecord` exceeds `node_size_cap`. Split the node or raise the cap (spill = backlog RFC 0014). |
| `bfs_depth_exceeded` | 400 | false | RFC 0007 | Variable-length path exceeded `bfs_depth_cap`. Lower the `*min..max` bound or raise the cap. |
| `namespace_not_found` | 404 | false | this RFC | Query/read against a prefix with no manifest (reader-side; writer creates on first write). |
| `namespace_in_use` | 409 | false | this RFC | Namespace drop refused while a live writer holds the epoch. |
| `invalid_index_spec` | 400 | false | RFC 0006 | Unknown tokenizer or tokenizer/value-type mismatch on index create. |
| `fenced_writer` | 503 | **true** | RFC 0001 | Writer lost its epoch (a newer writer opened the namespace); the `Db` returned `CloseReason::Fenced`. This node is shutting down. Retry routes to the new writer. |
| `write_on_reader` | 404 | false | this RFC | Write/admin route hit on a `--role reader` node (route not registered). Fix routing. |
| `internal` | 500 | false | any | Unexpected storage/encoding failure. |

Representative bodies:

```json
{ "error": "reader_behind", "retryable": true, "required_seq": 105, "current_seq": 101 }
```
```json
{ "error": "unindexed_property", "retryable": false,
  "property": "height", "hint": "declare an index or pass brute_force=true" }
```
```json
{ "error": "malformed_cypher", "retryable": false,
  "construct": "MATCH (a)--", "position": 11, "detail": "incomplete relationship pattern" }
```
```json
{ "error": "unsupported_cypher", "retryable": false,
  "clause": "WITH", "detail": "aggregation/projection pipelines are outside the v0 read subset",
  "see": "RFC 0013" }
```
```json
{ "error": "oversize_node", "retryable": false, "xid": "chunk:42",
  "encoded_bytes": 2101344, "cap_bytes": 1048576 }
```

`malformed_cypher` vs `unsupported_cypher` is the parse/scope split: a construct the grammar rejects is `malformed` (400); a construct the grammar accepts but the v0 planner refuses is `unsupported` (501, pointing at RFC 0013). `reader_behind` and `fenced_writer` are the only retryable codes — both mean "same request, different (or restarted) node."

## Fleet & deployment

```
                     ┌─────────────────────────────────────────┐
   writes/admin ───▶ │  writer node  (--role writer)           │
                     │  fenced Db · epoch CAS · owns latest_seq │
                     └───────────────┬─────────────────────────┘
                                     │  S3 prefix (one namespace = one SlateDB DB)
                     ┌───────────────┴─────────────────────────┐
                     ▼               ▼               ▼          ▼
              reader (DbReader) reader ...      reader      [LB fans reads]
                     ▲               ▲               ▲
   queries ─── load balancer ───────┴───────────────┘
```

### Writer singleton

Exactly one writer per namespace (D2/D13). This is an **operational invariant** — the deployer runs a single writer replica per namespace (e.g. a StatefulSet of size 1, or a lease in the orchestrator). It is **backstopped, not enforced, by turbolay**: SlateDB's manifest writer-epoch (CAS'd on open via an S3 conditional PUT) fences a zombie. If a second writer starts — redeploy overlap, partition — it bumps the epoch; the deposed writer's next manifest-touching write fails the CAS, the `Db` returns `CloseReason::Fenced`, and turbolay maps that to a `fenced_writer` (503) response and initiates graceful shutdown of that node. The sequence lineage never forks (RFC 0001 §zombie-writer). So correctness does not depend on the operator getting singleton-ness perfect; availability does.

### Reader fleet & `reader_behind` handling

Readers are stateless, unfenced `DbReader`s that poll the manifest (`manifest_poll_interval`) and replay WAL SSTs (RFC 0001). They scale horizontally behind an LB with no coordination. On a session-token query the reader gates on `durable_seq >= T`; if it can't catch up within `gate_timeout` it returns `reader_behind` (503, retryable) with `required_seq`/`current_seq`. Recommended handling, in order:

1. **Retry another reader** — a different replica may already be past `T` (readers advance independently). Cheapest; the LB re-dispatches.
2. **Fall back to the writer** — the writer always has the freshest local state, so a retry there cannot be behind. Last resort only: it recreates the hot spot the reader fleet exists to avoid (RFC 0001 rejects writer-routed reads as the default). Reserve for exhausted-retry or strict admin reads.

Clients keep the max `seq`/`latest_seq` they've seen and send it as `consistency.session`; a fresh caller with no token gets bounded-stale reads and never sees `reader_behind`.

### Health & readiness

- `GET /-/healthy` — liveness: process is up and the router responds. Always `200 OK` while running (opendata `handle_healthy`).
- `GET /-/ready` — readiness gate for the LB: **200 only when the node is caught up within a bound.** A reader is ready when `latest_seq - durable_seq <= ready_lag_bound` (its replay has caught the manifest within a configured seq/time bound) and storage answers a lightweight probe (opendata `check_storage`); otherwise `503`. A writer is ready when it holds the epoch (not fenced) and storage answers. This keeps a cold or badly-lagged reader out of the LB rotation until it can honor recent tokens, cutting the `reader_behind` rate.
- `GET /metrics` — Prometheus exposition (RFC 0017): `latest_seq`, per-index watermark lag, changelog-tail entries scanned, deleted-bitmap cardinality, per-hop frontier sizes, N+1 fan-out, and the **`reader_behind` rate** (a primary reader-fleet SLI). Wired through the same `MetricsLayer` the sister crates use.

Graceful shutdown drains in-flight requests on SIGTERM (K8s pod termination) / SIGINT before exiting; a fenced writer shuts down the same way after failing its CAS.

## Config

Mirrors opendata's `Config` (serde, `common::StorageConfig`, `serde_with` durations). One struct, role-tagged; reader-only fields are ignored on a writer and vice versa.

```rust
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Storage backend (InMemory | SlateDb{ object_store, .. }) — common crate.
    pub storage: common::StorageConfig,

    /// Node role. Writer opens a fenced `Db` + registers write/admin routes;
    /// reader opens a `DbReader` + read/query routes only.
    pub role: Role,                                   // Writer | Reader

    /// The single namespace this process serves (one SlateDB DB / one S3 prefix).
    pub namespace: String,

    /// Reader: manifest poll interval (freshness/staleness bound, RFC 0001).
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(default = "default_manifest_poll_interval")]  // 200 ms
    pub manifest_poll_interval: Duration,

    /// Reader: max wait in the session-token freshness gate before `reader_behind`.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(default = "default_gate_timeout")]            // 2 s
    pub gate_timeout: Duration,

    /// Reader: `/-/ready` returns 200 only when `latest_seq - durable_seq` is
    /// within this bound (keeps lagged readers out of the LB).
    #[serde(default = "default_ready_lag_bound")]         // 128 seqs
    pub ready_lag_bound: u64,

    /// Writer: reject an upsert whose encoded NodeRecord exceeds this (RFC 0004).
    #[serde(default = "default_node_size_cap")]           // 1 MiB
    pub node_size_cap: usize,

    /// Max variable-length path depth for BFS traversals (Q17, RFC 0007).
    #[serde(default = "default_bfs_depth_cap")]           // 6
    pub bfs_depth_cap: u32,

    /// HTTP listen port.
    #[serde(default = "default_port")]                    // 8080
    pub port: u16,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role { Writer, Reader }
```

CLI (`clap`) surfaces `--role`, `--namespace`, `--s3-bucket`/`--s3-region`/`--data-dir`/`--in-memory` (building `StorageConfig` exactly as opendata's `CliArgs::build_storage_config`), and `--port`; the rest come from a config file / defaults. `manifest_poll_interval` maps onto SlateDB `DbReaderOptions.manifest_poll_interval`; `node_size_cap`/`bfs_depth_cap` are the RFC 0004 / RFC 0007 guardrails surfaced as the `oversize_node` / `bfs_depth_exceeded` errors.

## Acceptance

1. **Role routing.** A `--role reader` router 404s `POST /v1/upsert`, `/v1/delete`, and every `/v1/admin/*`; serves `/v1/query`, `/metrics`, `/-/healthy`, `/-/ready`. A `--role writer` serves all. Driven via `into_router()` + `oneshot()`, no socket bound.
2. **Write ack shape.** `POST /v1/upsert` returns `{ ok: true, seq }` with `seq` monotone increasing across writes; a re-upsert of an existing `xid` reuses its uid and still advances `seq` (RFC 0004 §5).
3. **Read-your-writes over HTTP.** Upsert on the writer → capture `seq` → `POST /v1/query` with `consistency.session=seq` to a deliberately stale reader → the reader waits/replays and returns the new data; `latest_seq >= seq` in the response (RFC 0001 test 1).
4. **`reader_behind`.** Supply a token beyond a reader's reach within `gate_timeout`; assert `503` + `{ error: "reader_behind", retryable: true, required_seq, current_seq }`.
5. **Error taxonomy round-trip.** Each code maps to its documented status: unindexed filter → `400 unindexed_property` (and succeeds with `brute_force=true`); bad grammar → `400 malformed_cypher`; `WITH`/aggregation → `501 unsupported_cypher`; oversize upsert → `413 oversize_node`; over-deep var-length path → `400 bfs_depth_exceeded`.
6. **Fenced writer.** Open a second writer on the same namespace; assert the first writer's next `/v1/upsert` returns `503 fenced_writer` and the node begins graceful shutdown; the second continues the seq lineage (RFC 0001 test 6).
7. **Implicit namespace + admin.** First `/v1/upsert` to a fresh prefix creates the namespace; a reader querying a never-written prefix gets `404 namespace_not_found`. `POST /v1/admin/index` returns `backfilling`, and `GET /v1/admin/index` shows `watermark` catching up to `latest_seq` and transitioning to `live`.
8. **Readiness gate.** A reader lagging beyond `ready_lag_bound` returns `503` from `/-/ready`; once caught up, `200`. A fenced writer returns `503` from `/-/ready`.

## Alternatives considered

- **Separate writer and reader binaries.** Cleaner role boundary, but two build/release artifacts and drift risk between them. Rejected: one binary with `--role` (D9) is the opendata pattern (log's `--read-only`), shares all code, and keeps the route registration the single source of truth for which surface a role exposes.
- **Enforce writer singleton in-app (leader election / lease service).** Would prevent a second writer from ever starting. Rejected: that reintroduces the coordination layer D2 deletes (Raft/Zero). SlateDB's epoch CAS is the S3-only backstop; singleton-ness is an operational invariant, and correctness survives its violation via fencing (RFC 0001).
- **Multiplex many namespaces per process.** Fewer processes at high tenant counts. Deferred (Q19): one namespace per process is the isolation/atomic-drop win for POC scale; lazy-open shared hosting is a later optimization, not a v0 concern.
- **gRPC / a binary protocol.** Lower per-request overhead. Rejected for v0: JSON over HTTP matches the sister projects, is trivially inspectable, and the network edge is not the measured bottleneck (RFC 0017 targets S3 latency, not framing). A binary frontend can ride the same handlers later.
- **Server-side session state / a global freshness coordinator.** A stored per-client cursor or an external strongly-consistent store. Rejected (RFC 0001): the session token is a purely client-side monotonic counter; `m/latest_seq` + `durable_seq` already live in-band, so no coordinator and no server session are needed.
- **Build auth in v0.** Rejected as scope: v0 assumes a trusted network / mesh boundary. A token/mTLS layer slots in as middleware (`.layer(...)`) without touching the surfaces; noted, not designed.

## Final contract

- One binary, `--role {writer,reader}`, one namespace per process (one SlateDB DB / one S3 prefix, Q19). Writer fronts a fenced `Db` and registers the data-plane write routes + the admin plane; reader fronts an unfenced manifest-polling `DbReader` and registers read/query only — write/admin routes 404 on a reader (D2/D9/D13).
- **Data plane**: `POST /v1/upsert` (nodes+edges → one atomic batch, `{ ok, seq }`), `POST /v1/delete` (tombstone-and-filter, `detach` for eager), `POST /v1/query` (openCypher read subset → `{ results, latest_seq }`, with `consistency`, `brute_force`, `debug`).
- **Admin plane** (writer-only): `POST /v1/admin/index` (create + async backfill), `DELETE /v1/admin/index/{id}` (drop), `GET /v1/admin/index` (list + build status), namespace lifecycle (create/list/drop; **creation implicit on first write**).
- **Consistency on the wire**: writes return the logical `seq`; queries carry `{ consistency: { session } | { strict } }`; responses carry `latest_seq`; the reader gates on `durable_seq >= session` (RFC 0001).
- **Error taxonomy**: stable `{ error, retryable, ... }` codes with fixed HTTP statuses; only `reader_behind` (503) and `fenced_writer` (503) are retryable; `unsupported_cypher` (501) points at RFC 0013.
- **Fleet**: writer singleton = operational invariant + SlateDB epoch-CAS fencing backstop; reader fleet behind an LB; `reader_behind` → retry another reader, fall back to the writer only as a last resort; `/-/ready` = caught up within `ready_lag_bound`; `/metrics` wired to RFC 0017.
- **Config**: one serde `Config { storage, role, namespace, manifest_poll_interval, gate_timeout, ready_lag_bound, node_size_cap, bfs_depth_cap, port }` on `common::StorageConfig`, mirroring opendata.
- **Auth**: v0 = trusted network; a middleware auth layer is future work.
- Built on the opendata `server/` shape (`AppState`, always-on read routes, role-gated write routes, `.layer(Tracing).layer(Metrics)`, `axum::serve` + graceful shutdown, `into_router()` test seam).
