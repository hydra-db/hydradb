# Jepsen tests for Turbolay

Falsification harness for the graph kernel's read/write consistency claims.
Read `docs/jepsen-guide.html` first — it explains the method, the consistency
models, and why the object-store fault is the one that matters here.

## What is tested

Turbolay is not a consensus cluster. The object store is the source of truth,
SlateDB owns writer fencing and commit ordering, one writer is live at a time,
and every node serves reads. That shape determines which propositions are worth
attacking:

| # | Proposition | Falsified by |
|---|---|---|
| P1 | An acknowledged write is never lost | `set-full` reports **lost** |
| P2 | Two nodes racing to open the writer never both commit | **lost** or **duplicated** around a kill/pause |
| P3 | A `strong` read observes every write committed before its refresh | Knossos linearizability violation |
| P4 | A `causal` read carrying a bookmark observes that write, on any node | session checker read-your-writes violation |
| P5 | Reads never regress within a session | session checker monotonic-reads violation |
| P6 | The indexed-base + WAL-tail merge never drops or duplicates an edge | `set-full` **duplicated** / size mismatch while the indexer lags |
| P7 | A node cut off from S3 serves a stale-but-legal view or errors, never a wrong one | object-store partition + `strong` reads returning `:ok` with missing data |

## Findings so far

Recorded in full in `docs/jepsen-guide.html` §6. Two are properties of the
system worth knowing; three were harness bugs, which is the normal ratio.

| Finding | Kind | Detail |
|---|---|---|
| Fanning writes across nodes causes writer-fencing thrash | **system** | 80 × `l0_manifest_writer error=Fenced`, HTTP 500 on ~2/3 of writes. Fencing was *correct* — zero lost writes — but throughput collapsed. Clients need writer affinity; modelled in `routing.clj`. |
| A requested `timeout_ms` above `GRAPH_MAX_QUERY_RUNTIME_MS` is rejected, not clamped | **system** | Every read returned `429 resource_exhausted` (`src/client/service.rs:774`). The error reads like load shedding but is a misconfiguration. |
| Server cursors are bound to their `query_id` | **system** | Page 2 fails with `result cursor does not belong to this query request` unless the same `query_id` is sent for every page. A harness that stops at page 1 reports phantom data loss. |
| `nc/nemesis-package` constructs every fault regardless of `:faults` | harness | The file-corruption nemesis downloads an x86_64-only `bitflip` binary in `setup!`, aborting runs on arm64 even with `--nemesis none`. `nemesis.clj` picks packages by hand. |
| Object-store state leaked between runs | harness | `teardown!` clears node caches, but durable state lives in S3. The prefix keyed off a non-existent `:run-id` key, so all runs shared one path. |
| Run-id recomputed per test-map construction | harness | The test map is built more than once per `lein run`, so `setup!` and a later nemesis restart used *different* object-store prefixes. Final reads hit an empty database → a phantom `lost-count 426`. Now a top-level `def`. |
| `cu/grepkill!` leaves a stale pidfile | harness | `start-stop-daemon` then reports `:already-running` and never restarts the node — the heal looks successful while the port stays closed, and every post-fault read is `:connection-refused`. Use `cu/stop-daemon!`, plus a bounded `/readyz` wait in `start!`. |

> The last two together manufactured a 426-write "data loss" that never
> happened — the object store still held all 855 objects. Five of seven
> findings so far were harness bugs. That ratio is normal, and it is the whole
> argument for the escalation ladder.

## Verified results

120s per run, concurrency 10, 5 nodes, `strong` reads, 20s recovery before the
final read.

| Run | Verdict | Detail |
|---|---|---|
| `edge-set`, no faults | **pass** | 592 adds, 577 reads, 0 fail, 0 info, `lost 0` `stale 0` `duplicated 0` |
| `edge-set`, `kill` | **pass** | 405 acknowledged adds all stable; 160 writes correctly `:fail` as the writer died; `lost 0` `stale 0` `duplicated 0` |
| `edge-set`, `pause` | **pass** | 471 acknowledged adds, 0 fail, 10 indeterminate; `lost 0` `stale 0` `duplicated 0` |
| `edge-set`, `object-store` | **pass** | 507 acknowledged adds; **194 of ~504 reads indeterminate**, `stale 0` |
| `edge-set`, all four faults | **pass** | 216 acknowledged adds all stable under kill+pause+partition+object-store simultaneously; 875 ops correctly failed |
| `register` (`strong`), `kill,pause` | **pass** | Knossos found a valid linearization for **every** key; 1769 ops, 905 writes / 546 reads acknowledged, no `:valid? false` |
| `causal`, `kill,partition,object-store` | **pass** | 21 sessions, **0 read-your-writes violations, 0 monotonic-read violations**; `lost 0` `duplicated 0`; **`stale 235`** |

**All seven runs pass. Every proposition P1–P7 held.**

Reading the results:

- **P1 / P2 hold under both kill and pause.** Pause is the stronger result: a
  `SIGKILL`'d process cannot do damage, but a `SIGSTOP`'d one resumes holding a
  stale writer handle and cached epoch and tries to keep writing. Zero lost,
  zero duplicated across both.
- **P3 holds.** Knossos linearized every register key while nodes were being
  killed and paused underneath — the direct test of *"strong observes every
  durable write committed before that refresh completed"*.
- **P4 / P5 hold.** Across 21 sessions, every one of which hopped to a random
  node on every single operation, no session ever failed to see its own
  acknowledged write or lost an element it had already observed. The bookmark
  contract works across nodes.
- **P7 holds.** Under object-store partition, ~40% of reads became indeterminate
  and *none* became stale. A node severed from S3 but still reachable by clients
  errors rather than quietly serving a wrong view out of its local cache — the
  failure mode that would be most dangerous in production and hardest to notice.
- Writes were never spuriously acknowledged: `recovered-count` stayed 0 in every
  run, so nothing reported `:fail` was later found present.

### The most informative number: `stale 235` vs `stale 0`

The `causal` run recorded **235 stale observations** — an element present, then
absent, then present again. The five `strong` runs recorded **zero**, under
faults just as severe.

That is not a defect; `set-full` is configured with `:linearizable? false` and
staleness is exactly what causal consistency permits and strong consistency
forbids. It is the suite's best result, because it is *positive* evidence: the
two documented read modes are not merely labelled differently, they behave
measurably differently in precisely the documented way. A `causal` mode that
returned `stale 0` would suggest it was silently paying for strong reads; a
`strong` mode with `stale > 0` would be a bug. Neither happened.

## Layout

```
jepsen/
  docker/
    docker-compose.yml       MinIO + n1..n5 + control node
    node/Dockerfile          graph-node + sshd + iptables/gcc for nemeses
    control/Dockerfile       JDK 17 + Leiningen + gnuplot/graphviz
  turbolay/
    project.clj
    src/jepsen/turbolay.clj          CLI + test map
    src/jepsen/turbolay/db.clj       start/stop/kill/pause graph-node
    src/jepsen/turbolay/http.clj     HTTP query client + error classification
    src/jepsen/turbolay/edge_set.clj grow-only edge set      (P1 P2 P6 P7)
    src/jepsen/turbolay/register.clj linearizable register   (P3)
    src/jepsen/turbolay/causal.clj   bookmark session checks (P4 P5)
    src/jepsen/turbolay/nemesis.clj  faults incl. object-store partition
  docs/jepsen-guide.html
```

All five DB nodes point at the **same** `GRAPH_DATA_PATH` in the same MinIO
bucket. That is deliberate: it is the configuration in which SlateDB's writer
epoch fencing is load-bearing.

## Running

```bash
# 0. Build the graph-node runtime image (from the repo root)
docker build --target runtime -t turbolay:jepsen .

# 1. Bring the environment up
cd jepsen/docker
docker compose up -d --build

# 2. Run a test
docker compose exec control lein run test \
  --workload edge-set \
  --consistency strong \
  --nemesis kill,pause,object-store \
  --time-limit 300 \
  --concurrency 20 \
  --username root --password root

# 3. Browse results
docker compose exec -d control lein run serve      # http://localhost:8080
```

### Options

| Flag | Values | Default |
|---|---|---|
| `--workload` | `edge-set`, `register`, `causal` | `edge-set` |
| `--consistency` | `strong`, `causal` | `strong` |
| `--nemesis` | comma list of `kill`, `pause`, `partition`, `clock`, `object-store`, or `none` | `kill,partition,object-store` |
| `--nemesis-interval` | seconds between fault ops | `15` |
| `--recovery-time` | quiet seconds before the final read | `15` |
| `--concurrency-per-key` | register workload threads per key | `5` |

### Escalation ladder

Never start at the top. Each rung isolates one class of failure.

1. `--workload edge-set --nemesis none` — if this fails, it is not a distributed bug.
2. `--nemesis kill` — writer handover under clean death.
3. `--nemesis pause` — writer handover under unclean death. **The fencing test.**
4. `--nemesis object-store` — durability path severed, serving path intact.
5. `--workload register --nemesis kill,pause` — linearizability of `strong`.
6. `--workload causal --nemesis kill,partition,object-store` — bookmark contract.
7. Everything, `--time-limit 3600`, for soak.

## Results

Written to `turbolay/store/<test>/<timestamp>/`:

- `results.edn` — the verdict
- `history.txt` — the evidence every verdict derives from
- `jepsen.log` — harness log with nemesis timestamps
- `latency-raw.png`, `rate.png` — fault windows shaded
- `linear.svg` — only on a linearizability failure
- `graph-node.log` — per-node server logs, pulled back automatically

`:valid? :unknown` is **not** a pass — it usually means the checker timed out or
too many operations were indeterminate. Lower concurrency and re-run.

## Teardown

```bash
cd jepsen/docker && docker compose down -v
```
