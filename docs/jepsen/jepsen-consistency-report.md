---
id: JEP-001
title: Jepsen consistency testing of the read/write and query paths
status: complete
classification: verification-report
date: 2026-07-22
harness: jepsen/
guide: jepsen/docs/jepsen-guide.html
jepsen_version: 0.3.5
target_features: server-runtime (bolt + http-api + graphblas)
tags: [jepsen, consistency, linearizability, fencing, object-store, verification]
---

# JEP-001: Jepsen consistency testing of the read/write and query paths

## Summary

A Jepsen harness was built against the HTTPS query API of `graph-node` and run
across seven configurations: a no-fault baseline, four fault schedules on a
grow-only edge-set workload, a linearizable-register workload, and a
session-consistency workload for the bookmark contract.

**All seven runs passed. Propositions P1–P7 all held.** No lost writes, no
duplicated writes, no spuriously-acknowledged writes, and no stale reads under
`strong` in any run.

Three properties of the system were surfaced that are worth knowing
operationally (§5). Five defects were found in the test harness itself, two of
which together fabricated a 426-write "data loss" that had not occurred (§6) —
a ratio that is normal for Jepsen work and is the reason results are only
trustworthy after the no-fault rung is clean.

The most valuable single result is a *contrast* rather than an absence of
failure: `stale 235` under `causal` versus `stale 0` under `strong` at equal
fault severity (§7).

## 1. Why the standard Jepsen shape does not apply

Most Jepsen targets are consensus clusters, so the canonical fault is a network
partition between nodes. This system is not that:

- the object store is the source of truth; SlateDB owns writer fencing, WAL
  durability, and commit ordering;
- one writer per graph store, N readers; nodes are controllerless, and any node
  may lazily open the fenced writer;
- two read modes — `causal` (local durable view, refreshed only when a supplied
  bookmark requires it) and `strong` (refresh from object storage, then pin one
  snapshot);
- reads merge an indexed CSC base with the committed WAL tail when the async
  indexer lags.

Nodes barely communicate, so inter-node partitions are close to a no-op. The
dangerous configuration is a node **partitioned from the object store while
still reachable by clients** — it accepts connections and answers from local
cache with its durability path severed. No stock Jepsen nemesis does this, so
one was written (`jepsen/turbolay/src/jepsen/turbolay/nemesis.clj`).

The second-most-dangerous fault is `pause`, not `kill`. A killed process cannot
do damage; a `SIGSTOP`'d one resumes holding a stale writer handle and cached
epoch and tries to keep writing.

## 2. Propositions under test

| # | Proposition | Falsified by |
|---|---|---|
| P1 | An acknowledged write is never lost | `set-full` reports **lost** |
| P2 | Two nodes racing to open the writer never both commit | **lost** or **duplicated** around a kill/pause |
| P3 | A `strong` read observes every write committed before its refresh | Knossos linearizability violation |
| P4 | A `causal` read carrying a bookmark observes that write, on any node | session checker read-your-writes violation |
| P5 | Reads never regress within a session | session checker monotonic-reads violation |
| P6 | The indexed-base + WAL-tail merge never drops or duplicates an edge | `set-full` **duplicated** / size mismatch while the indexer lags |
| P7 | A node cut off from S3 serves a stale-but-legal view or errors, never a wrong one | object-store partition + `strong` reads returning `:ok` with missing data |

## 3. Environment

- 5 `graph-node` containers (`n1`–`n5`) + MinIO + a Jepsen control node, on one
  Docker bridge network.
- All five nodes point at the **same** `GRAPH_DATA_PATH` in the same bucket.
  This is deliberate: it is the configuration in which SlateDB's writer epoch
  fencing is load-bearing.
- Nodes run with `NET_ADMIN` (iptables partitions) and `SYS_TIME` (clock skew).
- Client: HTTPS query API, `POST /v1/graphs/default/query`, plaintext for the
  harness.
- Control node requires **JDK 21** — jepsen 0.3.5's dependency graph references
  `java.util.SequencedCollection`, which does not exist before 21.

## 4. Results

120 s per run (60 s for the baseline), concurrency 10, 5 nodes, 20 s recovery
before the final read.

| Run | Verdict | Detail |
|---|---|---|
| `edge-set`, no faults | **pass** | 592 adds, 577 reads, 0 fail, 0 info, `lost 0 stale 0 duplicated 0` |
| `edge-set`, `kill` | **pass** | 405 acknowledged adds all stable; 160 writes correctly `:fail` as the writer died |
| `edge-set`, `pause` | **pass** | 471 acknowledged adds, 0 fail, 10 indeterminate |
| `edge-set`, `object-store` | **pass** | 507 acknowledged adds; 194 of ~504 reads indeterminate, **0 stale** |
| `edge-set`, all four faults | **pass** | 216 acknowledged adds all stable under kill+pause+partition+object-store simultaneously; 875 ops correctly failed |
| `register` (`strong`), `kill,pause` | **pass** | Knossos linearized **every** key; 1769 ops, 905 writes / 546 reads acknowledged |
| `causal`, `kill,partition,object-store` | **pass** | 21 sessions, **0 read-your-writes and 0 monotonic-read violations**; `stale 235` |

`lost 0`, `duplicated 0`, `recovered 0` in every run.

Reading against the propositions:

- **P1 / P2 hold under both kill and pause.** Pause is the stronger result for
  the reason given in §1. Zero lost and zero duplicated across both means the
  epoch fencing survives the case it exists for.
- **P3 holds.** Knossos linearized every register key while nodes were being
  killed and paused underneath — the direct test of *"`strong` observes every
  durable write committed before that refresh completed"*.
- **P4 / P5 hold.** Across 21 sessions, each of which hopped to a randomly
  chosen node on *every* operation, no session failed to observe its own
  acknowledged write or lost an element it had already seen. A session pinned to
  one node could not detect a broken bookmark; these were not pinned.
- **P6 held indirectly** — no duplicates or size anomalies appeared while the
  indexer lagged, though no run deliberately forced a large indexer backlog.
- **P7 holds.** See §7.
- Nothing was spuriously acknowledged: `recovered-count` stayed 0 throughout, so
  no write reported `:fail` was later found present. This matters as much as the
  lost-write result — a database that applies writes it told you it rejected is
  as broken as one that drops writes it accepted.

## 5. Properties of the system worth knowing

### 5.1 Clients must have writer affinity

Jepsen spreads operations across all nodes by default. Against a single-writer
store that made all five nodes race to open the SlateDB writer, fencing each
other continuously: **80 × `l0_manifest_writer error=Fenced`** and HTTP 500 on
roughly two thirds of writes.

Fencing itself was **correct** — `lost-count 0`, no duplicates, nothing unsafe.
But write throughput collapsed, and a test that only re-measures thrash cannot
find anything else. Real clients must follow the Bolt routing table to the
current writer. The harness models this in `routing.clj`: writes go to one node
and advance on failure. That is also what makes killing that node a meaningful
*handover* test rather than noise.

### 5.2 A requested timeout above the server cap is rejected, not clamped

Every read returned `429 resource_exhausted`. The client requested
`timeout_ms: 30000` against a node configured with
`GRAPH_MAX_QUERY_RUNTIME_MS=20000`; the service rejects an over-cap request
outright (`src/client/service.rs:774`) rather than clamping to the maximum.

Defensible, but sharp: a client with a generous default timeout gets zero
successful queries against a conservatively configured node, and the error code
reads like load shedding rather than misconfiguration.

### 5.3 Server cursors are bound to their `query_id`

Paginated reads failed with
`400 invalid_request: result cursor does not belong to this query request`.
Omitting `query_id` makes the server generate a fresh one per HTTP request, so
page 2's cursor no longer matches the query that created it.

This is dangerous because of *how* it fails. A truncated read is not an error to
`checker/set-full`; it is evidence of catastrophic data loss. A client that
silently stops at page 1 will confidently report a database bug that does not
exist. Every page of one logical query must carry the same client-generated
`query_id`.

## 6. Harness defects found (and why they matter)

Recording these because each one produced a plausible-looking false result.

| Defect | Effect |
|---|---|
| `nc/nemesis-package` constructs all five stock packages regardless of `:faults`; the file-corruption nemesis downloads an x86_64-only `bitflip` release in `setup!` | Runs abort on arm64, or any host without egress, even with `--nemesis none`. Fixed by selecting packages by hand. |
| Object-store state leaked between runs | `teardown!` clears node caches, but durable state lives in S3. Prior runs' edges inflated result sets past the page size. |
| Run-id recomputed per test-map construction | The test map is built more than once per `lein run`, so `setup!` and a later nemesis restart used **different** object-store prefixes. Final reads hit an empty database. |
| `cu/grepkill!` leaves a stale pidfile | `start-stop-daemon` then reports `:already-running` and never restarts the node. The heal looks successful while the port stays closed and every post-fault read is `:connection-refused`. Use `cu/stop-daemon!`. |
| `db-nemesis` fires `:start` without waiting for readiness | Post-fault reads race a process that has not bound its listener. A bounded `/readyz` wait was added to `start!`. |

The last two together produced **`lost-count 426`** — essentially every
acknowledged write. The object store settled it: the original prefix still held
all 855 objects, written right up to the end of the run. Nothing had been lost.

The general lesson: for a database whose durable state lives outside the nodes,
"tear down the nodes" is not "reset the database", and a nemesis that *reports*
a successful heal has not necessarily performed one.

## 7. The most informative result

**`stale 235` under `causal` versus `stale 0` under `strong`**, at equal fault
severity.

This is not a defect. `set-full` runs here with `:linearizable? false`, and
staleness — an element present, then absent, then present again — is exactly
what causal consistency permits and strong consistency forbids.

It is the most valuable result in the suite because it is *positive* evidence
rather than an absence of failure: the two documented read modes are not merely
labelled differently, they behave measurably differently, in the documented
direction. Consider the counterfactuals. A `causal` mode reporting `stale 0`
would suggest it was quietly paying for strong reads and forfeiting the
performance the mode exists to provide. A `strong` mode reporting `stale > 0`
would be a straightforward correctness bug. Neither occurred — the sort of claim
a checker can support and a benchmark cannot.

The same run showed **194 of ~504 reads indeterminate and 0 stale** under
object-store partition, which is P7: a node severed from S3 but still accepting
client connections *errors* rather than quietly answering from its local cache.
That is the failure mode that would be most dangerous in production and least
likely to be noticed — a server that looks healthy and returns confident, wrong
answers.

## 8. Limitations

These results are weaker than the pass rate suggests, and should be read
accordingly.

- **Duration.** Runs were 60–120 s at concurrency 10. Published Jepsen analyses
  run for hours. Green here means "no anomaly surfaced in roughly 15 minutes of
  fault injection", not "correct". The soak rung
  (`--time-limit 3600`) has **not** been run.
- **Clock skew is wired but untested.** `SYS_TIME` is granted and the nemesis is
  selectable, but no run exercised `--nemesis clock`. Any lease, TTL, or
  timestamp that fencing implicitly depends on is therefore unprobed.
- **Elle is unused.** The HTTP API is auto-commit only, so no multi-statement
  transaction dependency cycles can be inferred. This changes the day
  transactions ship, and Elle should be added then.
- **P6 was not stressed.** No run deliberately forced a large CSC indexer
  backlog, so the indexed-base + WAL-tail merge was only exercised incidentally.
- **Bolt is untested.** All operations went over the HTTPS API; the Bolt adapter
  shares the same `ClientQueryService` but was not driven directly.
- **Single cell, single graph.** No run exercised multiple cells, namespaces, or
  scoped routing.

## 9. Reproducing

```bash
docker build --target runtime -t turbolay:jepsen .
cd jepsen/docker && docker compose up -d --build

docker compose exec control sh /jepsen/run-suite.sh      # rungs 1-5
docker compose exec control sh /jepsen/run-workloads.sh  # rungs 5-6
```

Results land in `jepsen/turbolay/store/<test>/<timestamp>/`. `:valid? :unknown`
is **not** a pass — it usually means a checker timed out or too many operations
were indeterminate to conclude anything.

See `jepsen/README.md` for options and the escalation ladder, and
`jepsen/docs/jepsen-guide.html` for the method, the consistency-model lattice,
the pitfalls that produce false results, and a reading list.
