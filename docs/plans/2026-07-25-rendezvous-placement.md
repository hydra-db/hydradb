---
title: Rendezvous placement for cell writers
status: step-2-complete
date: 2026-07-25
branch: Turbolay-V3.5
base_commit: 3bacd71
head_commit: ea942ec
tags:
  - routing
  - placement
  - single-writer
  - fencing
---

# Rendezvous placement for cell writers — code plan

Sections 1 and 2 landed in `7b0d340`, with the finalizer following in `ea942ec`.
Sections 3–5 are unstarted.

All twelve §7 decisions are settled; nothing is open. Decisions 10–12 fix the
`heartbeat.rs` signatures and were taken before commit 1 for that reason. All of Phase 1
ships in a single release as seven staged commits (§7.8) — the heartbeat
publisher ahead of the routing change, because decision 4 deletes the probe. The
reasoning behind every settled decision lives in
`docs/plans/2026-07-25-rendezvous-placement-open-questions.md`; §7 carries only
the outcome.

Fixes the prod incident in `cell-writer-fencing-pingpong`: three graph-nodes
ping-ponging one cell's SlateDB writer epoch, because nothing decides which node
*should* be the writer.

## Sources — read these before changing this plan

The analysis behind every choice below already exists. Do not re-derive it.

| Source | Holds |
|---|---|
| `interactive/write-routing-problem.html` | The write path traced Bolt socket → WAL commit with file:line refs at each step. Ten numbered failure modes (F-01…F-10) with severities. Requirements R1–R7, which the option scoring uses. |
| `interactive/write-routing-solutions.html` | How TiDB/TiKV routes (PD, region cache, `NotLeader`, DDL owner). Four candidate ownership sources (OPT-1…OPT-4) scored against R1–R7. The three-layer proposal this plan implements. Open questions. |
| memory `cell-writer-fencing-pingpong` | The prod incident itself: log signature, root cause, impact. Its `state.rs` line numbers are stale by ~45 lines; everything else verified accurate as of `511aece`. |

Reference implementations, already located — the useful files, not the repos:

| Path | Why |
|---|---|
| `../sleet/src/placement.rs` | The rendezvous implementation this copies: FNV-1a 64, frozen key encoding, tie-break, and the golden + minimal-disruption tests. |
| `../sleet/rfcs/0001-design.md` | Sections "Heartbeats", "Placement", "Failure behavior", "Coordinator fencing" — the liveness rules and the convergence bound. |
| `../tidb-master/pkg/store/copr/batch_coprocessor.go:1556`, `pkg/store/copr/mpp.go:187` | `InvalidateCachedRegionWithReason` — the invalidate-with-reason pattern behind touch point (c). |
| `../tidb-master/pkg/ddl/job_scheduler.go:132`, `pkg/ddl/job_submitter.go` | Submit-to-queue / owner-executes, the model behind the deferred forwarding follow-up. |
| `../tidb-master/pkg/owner/manager.go:179-262` | The etcd election we are *not* building, and its mixed-version hazard (issue #54689) — the argument for why. |

## The idea in one line

`owner(scope, cell) = argmax over live nodes of H(scope ‖ cell ‖ node)` — no
stored assignment, no lease, no election service. Every node computes the same
answer from the same object-store LIST.

## Two phases, and why the split matters

**Phase 1 stops the bleeding without touching the query path.** All three nodes
today have the same `GRAPH_CELL_ID`, so every client already targets one cell.
Once rendezvous picks one owner for that cell and the other two decline, the
ping-pong stops. This needs no resolver or planner change.

**Phase 2 unlocks write scaling** and is the larger change: a write cannot name
its cell today (`HierarchicalClientDatabaseResolver` copies `cell_id` from node
config, `src/client/service.rs:112-116`), so a per-cell map is correct but
unused. Deferred deliberately — it is a separate review.

Everything below is Phase 1 unless marked.

---

## 1. New crate: `turbolay-placement` — LANDED

`crates/placement`, package `turbolay-placement`, `publish = false`.

The root **stays** the `slatedb-graph-kernel` package and merely gains a
`[workspace]` table. This is the whole reason CI needed no rewrite: in a
virtual manifest every one of the ~40 bare `cargo test --features …`
invocations across `ci.yml`, the `justfile` and `scripts/` is an error, whereas
with a root package they still resolve to the root. Verified, not assumed —
`cargo check --locked --all-targets --features chaos-harness` with no `-p`
still builds `slatedb-graph-kernel v0.1.0` and nothing else.

`exclude = ["docs"]` keeps the 26 vendored Quint/CosmWasm sample manifests
under `docs/formal-methods-docs/` out of the workspace. They are gitignored, so
CI never saw them, but they are present in local trees.

Two consequences that did need work:

- **`Cargo.lock` is regenerated** (+4 lines, the new package stanza) and must
  be committed in the same change. Every build path uses `--locked` —
  `ci.yml`, `Dockerfile:21`, `Dockerfile:27`, `justfile`, `scripts/ci_local.sh`
  — so a stale lock fails every job.
- **Bare cargo lines select the root package only**, so the member would be
  silently unlinted and untested. Fixed by *added* lines, not edited ones:
  `-p turbolay-placement` clippy + test steps in `ci.yml`, a `test-placement`
  recipe wired into `just ci`, and the same pair in `scripts/ci_local.sh`.
  Also `crates/**` added to `container.yml`'s PR path filter, and
  `justfile`'s `cargo fmt` → `cargo fmt --all` so local and CI formatting no
  longer disagree.

The crate has **no dependencies** today. The frozen hash is pure functions over
`&str`. When `heartbeat.rs` lands it adds `slatedb` — for the re-export only,
since `object_store` is not a direct dependency of the kernel either (it is
consumed through `slatedb::object_store`, `src/lib.rs:8`) and
`Arc<dyn ObjectStore>` will not unify across the crate boundary otherwise.

No dependency on `slatedb-graph-kernel` — placement must not know about graphs,
shards, or queries. That keeps it testable against `InMemory` alone.

```
crates/placement/
  src/
    lib.rs         ✅ owner/rank/score re-exports
    hash.rs        ✅ frozen rendezvous scoring
    heartbeat.rs   ⬜ object names, body type, the LIST boundary
    liveness.rs    ⬜ live set = configured membership ∩ fresh heartbeats
```

`liveness.rs`, not `directory.rs` — decision 12. The kernel already has an
`ObjectStoreNodeDirectory` and it is the *other* half of the intersection.

The split follows sleet: `heartbeat.rs` is names, the body type, and the single
impure `list_heartbeats`; `liveness.rs` is pure decision functions over
pre-computed ages (decision 10). The publisher loop is not in this crate at all —
it lands in `graph-node.rs` in commit 2.

## 2. The frozen hash — LANDED

`crates/placement/src/hash.rs`. Copied from `sleet/src/placement.rs`, which is
the proven version of exactly this, with sleet's `(database, service)` pair
replaced by `(scope, cell)` — sleet's `Service` enum and its `mirror` triple
variant (`score_target`/`rank_target`/`owner_target`) have no analogue here and
were dropped. FNV-1a 64, key encoding and tie-break frozen as a wire format:

```
score(scope, cell, node) = mix64(fnv1a64(scope ‖ 0x00 ‖ cell_id ‖ 0x00 ‖ node_id))
rank                     = descending by score, ties broken by node_id ascending
owner                    = rank[0], or None when no node is live
```

### The finalizer, and why it was added after the fact

`mix64` is splitmix64's three shift-xor-multiply rounds. It was **not** in the
version that landed in `7b0d340`; it was added once the distribution was
measured rather than assumed.

FNV-1a ends on a single multiply, so keys that differ only in their final byte
come out agreeing in their high bits and the ranking is settled by a handful of
low ones. Node ids differing only in the final byte is not a corner case — it is
what a StatefulSet produces:

| | 3 nodes, 20 000 cells | max/min |
|---|---|---|
| plain FNV-1a, `graph-node-0..2` | 5000 / 5000 / 10000 | **2.00** |
| plain FNV-1a, unrelated names | 6347 / 6932 / 6721 | 1.09 |
| **FNV-1a + splitmix64**, `graph-node-0..2` | 10040 / 9980 / 9980 | **1.006** |

One node owning half the fleet, at every scale, not converging with cell count.
Reordering the key so the node id is hashed first does **not** fix it (measured
1.42 at three nodes, 2.14 at six); only the finalizer does.

Phase 1 is unaffected either way — one cell, and all that matters is that every
node agrees. It would have bitten in Phase 2, as soon as cells outnumber nodes.
Changing it costs one re-pin of the golden values today because nothing consumes
placement yet; after Phase 1 ships it would be a fleet-wide reshuffle with a
writer handover per cell. `ownership_is_balanced_across_similar_node_ids` guards
the property, and would fail against the old hash.

`scope` is `GraphScope`'s `Display` — `"{namespace}/graphs/{graph_id}"`
(`src/core/namespace.rs:268-272`). **This becomes a compatibility surface the
moment two versions run together**, so it gets a golden test with pinned hex
values, same as sleet's `scores_are_frozen`.

```rust
pub fn score(scope: &str, cell_id: &str, node_id: &str) -> u64;
pub fn rank<'a>(scope: &str, cell_id: &str, live: &[&'a str]) -> Vec<&'a str>;
pub fn owner<'a>(scope: &str, cell_id: &str, live: &[&'a str]) -> Option<&'a str>;
```

`owner` returning `None` means *placement has no opinion*, not *nobody may
write* — it is the third arm of touch point (b), where a node promotes itself
because no owner is live.

Eight tests, all passing. The pinned values, which are now a compatibility
surface — **re-pinned when the finalizer was added**, so any tree still
asserting the `0xf0dc…` generation predates it:

| `scope` | cell | node | score |
|---|---|---|---|
| `acme/graphs/social` | `cell-a` | `graph-node-0` | `0xf446a8f8dd607f2f` |
| `acme/graphs/social` | `cell-a` | `graph-node-1` | `0x535527081c8d0fb0` |
| `acme/graphs/social` | `cell-b` | `graph-node-0` | `0x36c7be50a933a232` |
| `acme/graphs/other` | `cell-a` | `graph-node-0` | `0x29d54f9b37598022` |

Two notes for whoever touches this next. The minimal-disruption and
runner-up tests use `cell-b` deliberately: under `cell-a` the ranking happens
to come out in lexical node order, so a test written against it would pass even
if `rank` ignored the score entirely. And pinning against `GraphScope::Display`
freezes *that* impl (`src/core/namespace.rs:268-272`) as a wire format too —
changing `"{namespace}/graphs/{graph_id}"` re-shuffles every cell in the fleet.

## 3. Heartbeats

```
<base>/_graph_nodes/v1/<node-id>
```

Liveness comes from the object's `LastModified`, from object storage — never a
local clock. A node is live if its heartbeat is younger than `heartbeat_timeout`.
Placement reads only the LIST result (name + timestamp); the body is
observability and is never fetched on the placement path.

Every liveness function receives the comparison time as a parameter and reads no
clock of its own — decision 10, which fixes the signatures below.

Following sleet: **a node always counts itself live.** If its own heartbeat looks
stale it has no reliable proof it should stop.

Publication is **gated on `AdminState.ready`** (decision 4): a node publishes
only while ready, deletes its object when it goes unready, and deletes it in the
SIGTERM handler before draining. Heartbeat freshness is then the only readiness
signal Bolt clients have, which is why it must carry readiness and not merely
liveness.

Defaults: `heartbeat_interval` **5s**, `heartbeat_timeout` **15s** (decision 5),
config with startup validation. Convergence after a node dies is bounded by
~`heartbeat_timeout`.

Extension point, not built now: if nodes ever serve different cell subsets,
encode that in the object *name* (sleet's service-letter trick) so placement
still needs no GETs.

## 4. Wiring into the kernel — four touch points

**a. `ObjectStoreBoltRoutingTableProvider::routing_table`**
(`src/client/bolt/routing.rs:222-250`)

Currently ignores both parameters. Change `_target` to `target` and:

- `WRITE` → the address of `owner(target.scope, target.cell_id, live)`
- `READ`, `ROUTE` → all live nodes, unchanged

Also **drop the per-`ROUTE` `/readyz` fan-out** (`routing.rs:152-168, 196-218`)
in favour of heartbeat freshness. That removes N TCP connects per routing
refresh, and — the real reason — makes liveness consistent between two clients
asking at the same instant instead of computed per-caller. This deletes
`reachable_nodes`, `probe_node_readiness`, and `replace_address_port`.

**b. `RoutedGraphCluster::ensure_local_writer`** (`src/engine/cluster.rs:375-390`)

The rule that ends the duel:

> A node must not promote itself for a cell it does not own, unless the computed
> owner is not live.

```rust
match placement.owner(scope, cell_id) {
    Some(o) if o == self.local_node_id => shard.promote_to_writer(..).await,
    Some(o)                            => Err(GraphError::NotCellWriter { cell_id, owner: o }),
    None                               => shard.promote_to_writer(..).await, // no live owner
}
```

This one branch is what converts "any node steals the epoch on demand" into
"only a node with a placement reason claims it".

**c. New error + Bolt mapping** (`src/core/error.rs`, `src/client/bolt.rs`)

`GraphError::NotCellWriter { cell_id, owner }` maps to
`Neo.ClientError.Cluster.NotALeader`, carrying the owner as a hint — TiKV's
`NotLeader{leader_hint}`. Drivers already know this code: discard the routing
table, re-route, retry. Today the Bolt module has exactly one Neo code
(`bolt.rs:1366`), so a fenced write is opaque and the driver retries into the
same wrong node.

HTTP clients cannot re-route, so `ClientHttpServer` returns 421 with the owner in
the body. Forwarding over `QueryServiceEndpoint` is the better answer for HTTP
and is a follow-up, not this change.

**d. Fenced-writer backoff** (`src/core/state.rs:235-246`)

`refresh_writer_fence` currently just drops the handle, so the next write
reopens immediately. Add: on `CloseReason::Fenced`, wait one `heartbeat_interval`
(5s) before re-promoting, **and reset the exponential backoff to 1s** — a fence
is view skew, not a failure. Decision 6 in §7 has the three rules to match, all
from sleet's handler. Combined with (b), mutual fencing is then bounded by view
convergence rather than by nothing.

**e. `graph-node.rs`** — start the heartbeat task; build the placement handle
from `GRAPH_NODE_ID` and the node directory; pass it to the routing provider and
the cluster.

## 5. Tests

Three harnesses already exist and none of this needs new integration scaffolding:

- **`src/tests.rs:3134` `routed_cluster_uses_slatedb_writer_fencing`** stands up
  two `RoutedGraphCluster`s with different `local_node_id` over one shared
  `InMemory` store and asserts the stale writer gets
  `ErrorKind::Closed(Fenced)`. The regression test is that, with a third node and
  concurrent writes.
- **`RoutedGraphCluster` already carries `local_node_id` and `directory`**
  (`cluster.rs:165-166`, both flowing through
  `open_promotable_scoped_with_memory_options`), so touch point (b) is testable
  in-process with an injected live set and no heartbeat plumbing at all.
- **`examples/fence_worker.rs`** — incumbent/takeover/reader modes coordinating
  through a signal dir, for real cross-process epoch contention. `jepsen/` for
  the fleet-level property.

| Test | Where | Guards |
|---|---|---|
| Golden hash values, pinned hex | `turbolay-placement` | the wire format |
| Remove a node, order of the rest preserved | `turbolay-placement` | minimal disruption |
| 3 similar node ids over 30 000 cells stay within 10% | `turbolay-placement` | the finalizer — fails at 2.00 without it |
| Heartbeat expiry, self-always-live | `turbolay-placement` | liveness rules |
| LIST failure inside grace keeps the view; past grace sheds and unpublishes | `turbolay-placement` + `FaultStore` | decision 7 — the bound |
| LIST fails while PUTs still land → node withdraws its heartbeat | `turbolay-placement` + `FaultStore` | decision 7's partial failure — the permanent-refusal trap |
| Never-published peer counts live for one timeout after start, dead after | `turbolay-placement` | decision 8 — the rolling restart |
| `interval >= timeout` is rejected at startup | `turbolay-placement` | decision 5's validation |
| Publishes while ready, DELETEs on unready and on SIGTERM | `turbolay-placement` + `graph-node` smoke | decision 4 — readiness rides the heartbeat |
| Routing table names the rendezvous owner for WRITE; `READ`/`ROUTE` unchanged; no probe fan-out remains | `bolt/routing` tests | touch point (a) |
| Non-owner write returns `NotCellWriter`, not a promotion; owner promotes; `None` owner promotes | `cluster` tests | touch point (b), all three arms |
| Maps to `Neo.ClientError.Cluster.NotALeader` with the owner hint; HTTP returns 421 | `bolt` tests | touch point (c) |
| On `Fenced`: waits one interval, resets backoff to 1s, retries without re-checking ownership | `core/state.rs` unit | decision 6, all three rules |
| Advisory record written only after a successful promotion, never read to decide one | `cluster` tests | 3a stays advisory |
| **3 promotable nodes, 1 cell, concurrent writes → epoch growth bounded** | integration | **the prod regression** |

The last row is the one that matters — it is the incident, reproduced. It asserts
a **bound on epoch growth, not zero re-fences**: decision 6.3 accepts that a
fenced node re-fences the winner once more before converging, so a test demanding
a single epoch bump would fail correctly-behaving code. Convergence within a
bound is the invariant; that bound is what the test pins.

Every time-dependent row above is a pure-function table test running in
microseconds, because of decision 10. None of them sleep.

## 6. Explicitly out of scope

- **CAS lease objects.** Superseded by decision 3 in §7. The advisory record
  (3a) is in scope; an authoritative CAS-on-open (3c) is **dropped**, not
  deferred — neither the fork nor upstream 0.14.1 exposes any epoch option on
  `DbBuilder`, and sleet demonstrates the bound is reachable without one.
- Phase 2: cell addressability on the wire.
- Rebalance dampening. Rendezvous moves ownership the instant the live set
  changes, so a flapping node reclaims its cells each time it returns, each
  reclaim costing a writer open. PD has leader-transfer scheduling for this;
  rendezvous has nothing. A minimum-tenure rule probably belongs here later.
- Cell splitting. Cells are static config (`GRAPH_CELLS`); routing a hot cell
  correctly to one writer is precisely what makes it a bottleneck.
- Multi-cell write atomicity.

## 7. Decisions

### 1. Convert the root to a Cargo workspace — YES, decided

Root stays the package, `crates/placement` is the sole member. The distinction
that matters is **virtual workspace vs root-package workspace**: a virtual
manifest breaks every bare `cargo … --features …` in CI, a root-package
workspace does not. Implementation and verification in §1.

CI needed **no edits**, only additions. Two real costs, both small and both
paid: the regenerated `Cargo.lock` must ship in the same commit (everything
runs `--locked`), and the member needs its own `-p` lint/test lines or it goes
silently uncovered.

### 2. Crate name — `turbolay-placement`, decided

The name is a fence around what the crate is allowed to grow into. What moves
out is placement, heartbeats and liveness; routing-*table construction* stays
in `client/bolt/routing.rs`, because it needs `ClientQueryTarget` and
`BoltRoutingServer`, which are kernel types. Naming it `turbolay-routing` would
invite that Bolt code to migrate in later and drag the kernel's types with it —
the dependency cycle this split exists to prevent.

Rejected `slatedb-graph-placement` (consistency with `slatedb-graph-kernel`):
placement over object storage is not SlateDB-specific. It will depend on
`slatedb` only to re-export `object_store`, and if that re-export is ever
replaced by a direct dep the name becomes a lie.

### 3. Lease — 3a, advisory record only. Decided

Full reasoning in Q1 of the open-questions record; the short version:

An authoritative lease is **not implementable** against SlateDB as it stands.
The writer epoch is readable (`VersionedManifest::writer_epoch()` via
`DbStatus.current_manifest`) and is already a CAS-backed monotonic lease — but it
carries **no identity**, and there is **no fence-before-open hook** on
`Db::builder`. `build()` claims a new epoch unconditionally. A lease object
therefore cannot *prevent* anything, because SlateDB never consults it.

**3a** — after a *successful* promotion, and only then:

```
PUT <base>/_cell_writers/v1/<cell_id>
    {"node_id":"graph-node-1","epoch":18,"at":"2026-07-25T14:32:07Z"}
```

Read on exactly two paths, both off the write path: logging a
`CloseReason::Fenced`, and building the `NotCellWriter { owner }` hint in touch
point (c). **Never consulted to decide whether to promote** — that is
rendezvous' job, and consulting it would be 3b, which is a read-then-act race
that looks like it works.

3b rejected: buys nothing 3a doesn't, adds a GET to the write path, still cannot
stop a partitioned node.

**3c (CAS-on-open in SlateDB) dropped**, not deferred. Three reasons: sleet runs
this architecture at fleet scale with no CAS-on-open and converges, model-checked
(`../sleet/specs/coordination.fizz`), and our design is *stricter* than sleet's —
we check ownership on every promotion, sleet once at task spawn. Neither the fork
nor upstream 0.14.1 has any epoch option on `DbBuilder`, so it is an upstream
feature request that would gate a prod fix on a dependency change. And the
incident's actual pain was diagnostic, not correctness.

**Recorded so this is never re-read as a guarantee:** the SlateDB writer epoch
remains the only authority. The record answers "who last successfully promoted",
not "who holds the writer now". If it disagrees with the manifest, the manifest
is right.

### 4. Deleting the `/readyz` client-side fan-out — YES, decided

**Keep, untouched** — the endpoint at `graph_node/admin.rs:48` and every
non-routing consumer: the k8s `readinessProbe` (`node-statefulset.yaml:118-120`,
`indexer-deployment.yaml:96`, `multinode_k3s.sh:114`), `scripts/runtime_smoke.sh:51`,
the bounded wait in the Jepsen harness.

**Delete** — only the client-side fan-out inside the routing provider:
`reachable_nodes`, `probe_node_readiness`, `replace_address_port`
(`routing.rs:152-168, 196-218`). Also the `reachable.first()` writer fallback
(`routing.rs:236-241`), which moves whenever a probe flaps and is half the
instability.

The reason is consistency, not cost. The probe is computed **per caller**, so
two drivers asking at the same instant can get different answers — and
rendezvous only works if every node computes ownership from the *same* live set.
One LIST gives that; N probes cannot.

The catch that makes this non-obvious: Bolt clients do **not** reach nodes
through a k8s Service. They connect directly to pod addresses from
`GRAPH_BOLT_NODE_ADDRESSES` (`charts/turbolay/templates/configmap.yaml:44`), so
k8s readiness gates no Bolt traffic at all. That is why `routing.rs` probes
independently, and why deleting the probe alone would advertise unready nodes.

**The deletion and its replacement are one change, not two.** A node publishes
its heartbeat *only while `AdminState.ready` is true*, and deletes it on graceful
shutdown or when it goes unready:

```
every heartbeat_interval:   ready?  -> PUT    <base>/_graph_nodes/v1/<id>
                            not?    -> DELETE <base>/_graph_nodes/v1/<id>
SIGTERM:                            -> DELETE <base>/_graph_nodes/v1/<id>, then drain
```

Readiness then propagates through the same channel as liveness. One accepted
regression: a hard-crashed node goes from ~250 ms detection to up to
`heartbeat_timeout` = 15s. Graceful restarts — the common case — stay immediate,
because the node deletes its own heartbeat before exiting.

### 5. Heartbeat interval and timeout — 5s / 15s, decided

Config, with sleet's startup validation (`interval > 0`, `interval < timeout`,
`../sleet/src/config.rs:1018-1024`) so a misconfiguration that would make every
node permanently dead is rejected at startup rather than in production. This
supersedes the 10s/30s in §3 and the 2s/6s floated there.

| | value |
|---|---|
| worst-case dead-node detection (hard crash) | 15s |
| graceful shutdown detection | immediate, via heartbeat DELETE |
| placement convergence after a node dies | ~15s |
| fence backoff in touch point (d) | 5s — one interval, per sleet |
| heartbeat PUT cost, 3 nodes | ~52k/day, ~$8/month |

15s partially absorbs a ~10s flap, which 6s would have churned on. If flapping
still hurts, the fix is a minimum-tenure rule, not a longer timeout — §6
follow-up, not built now.

### 6. Fence handling must match sleet exactly

Touch point (d) currently under-specifies this. Sleet's handler
(`../sleet/src/daemon.rs:458-472`) does three things, and all three matter:

1. **Resets `backoff` to 1s** rather than advancing the exponential ladder — a
   fence is view skew, not a failure. Without this, repeated fences ride the
   ladder to `MAX_BACKOFF` and a node that is merely converging looks dead.
2. **Waits exactly one `heartbeat_interval`**, sized to give *the rival* time to
   refresh its view and stand down (`daemon.rs:391-394`). Not a politeness delay.
3. **Retries unconditionally**, without re-checking ownership. Sleet accepts that
   a fenced node re-fences the winner once more before converging.

### 7. A failed node LIST — bounded grace, then shed. Decided

Sleet keeps its assignments on a failed LIST (`daemon.rs:243`). For a writer that
is an unbounded duel: a partitioned node can never learn it lost, so it
re-promotes forever. Shedding on the *first* failure is the opposite extreme and
drops a healthy writer on one throttled request. Neither is right.

```
grace = heartbeat_timeout (15s), measured from the last successful LIST

within grace  -> serve the cached live set, keep retrying with backoff
past grace    -> go unready: stop publishing, DELETE the heartbeat
              -> refuse promotion: NotCellWriter with no owner hint
```

The grace is `heartbeat_timeout` and not a new knob because peers take over that
long after the last successful PUT, and in a partition LIST and PUT fail
together. A shorter grace leaves a window where nobody owns the cell; a longer
one leaves a duel; equal makes them coincide.

**Shedding must withdraw the heartbeat**, and that is the part that is easy to
miss. In a partial failure — LIST throttled, PUTs still landing — a node that
sheds while still publishing stays the computed owner for everyone else, so peers
route writes to it and it refuses every one, permanently, with nothing to time
out. Withdrawing makes peers take over. It also falls out of decision 4's
readiness gating for free: a node that cannot read the live set is not ready.

Worst case for the cell: 15s grace + peer detection (immediate if the DELETE
lands, another 15s if the store is fully unreachable) — **≤ 30s**, bounded either
way.

`last_successful_list` is a monotonic local `Instant` measuring elapsed duration
only; cross-node comparison stays on object-storage `LastModified`. An **empty**
LIST is a valid answer, not a failure, and must not share the failure path.

### 8. Build scope — all of Phase 1, one release. Decided

Seven commits, shipped together. No intermediate version reaches prod.

| # | Commit | Runtime effect |
|---|---|---|
| 1 | §3 crate — `heartbeat.rs`, `liveness.rs` | none |
| 2 | **publisher** — readiness-gated heartbeat task, DELETE on SIGTERM | writes objects nothing yet reads |
| 3 | **(a)** routing — live set from LIST, `WRITE` names the owner, fan-out deleted, `with_preferred_writer_node` deleted | first client-visible change |
| 4 | **(b)+(c)** don't-promote rule + `NotALeader` mapping | **the duel stops** |
| 5 | **(d)** fence backoff + reset to 1s | bounds mutual fencing |
| 6 | **3a** advisory record + fenced-log attribution | next incident readable in one line |
| 7 | **§5** regression test | none |

The publisher is split out of (e) and placed ahead of (a) because (a) deletes the
probe and cannot function until heartbeats exist. Commits 3 and 4 are the pair
that must not be separated, and are where the review effort belongs.

Shipping one release removes most of the mixed-version hazard but not all of it —
a rolling restart replaces pods one at a time. So `liveness.rs` distinguishes
**never-published** from **expired**: for the first `heartbeat_timeout` after
process start, a configured peer with no heartbeat object counts as live. Only
expiry is evidence of death; the never-seen case degrades to today's behaviour
instead of concluding the rest of the fleet is dead.

### 9. Delete `with_preferred_writer_node` — decided

It has no production caller — `graph-node.rs:115-116` never calls it, so the
field is always the address map's first key (`routing.rs:76-80`) and its only
caller is `bolt/tests.rs:1373`. Keeping it once (b) enforces ownership creates a
routing loop no timeout breaks: routing advertises the override,
`ensure_local_writer` enforces rendezvous, and the driver bounces between them.

Deleted **in commit 3**, with the probe fan-out, not deferred past it — commits 3
and 4 are exactly what makes the override contradictory, so carrying it through
them is how the loop reaches prod.

If a pin is ever wanted it belongs *inside* `owner()` in `turbolay-placement`, so
routing and `ensure_local_writer` cannot disagree. Full argument in section B of
the open-questions record.

### 10. Time is a parameter, never a clock read — decided

Nothing in `turbolay-placement` calls `Utc::now()` or `Instant::now()`.

Sleet does this better than "pass `now` everywhere", and reading it
(`../sleet/src/root.rs:134-219`) improved the decision: **collapse the timestamp
to a `Duration` age at the LIST boundary, and every function below it is pure and
time-free.** `list_heartbeats` is the one impure function and the only thing that
takes a `now`; `HeartbeatEntry` carries `age: Duration`; the decision functions
take no time argument at all.

```rust
// The one impure boundary — the only thing that knows what time it is.
pub async fn list_heartbeats(store: &dyn ObjectStore, prefix: &Path,
                             now: DateTime<Utc>) -> Result<Vec<HeartbeatEntry>>;

// Everything below: pure, synchronous, no clock, no Instant, no DateTime.
pub fn youngest_per_node(entries: &[HeartbeatEntry]) -> BTreeMap<&str, &HeartbeatEntry>;
pub fn live_nodes(entries: &[HeartbeatEntry], timeout: Duration) -> Vec<NodeView>;
```

Decision 7's `last_successful_list` and decision 8's process-start instant arrive
as elapsed `Duration`s for the same reason.

The payoff is visible in sleet's own tests: `node_view_dedups_youngest_and_drops_dead`
(`root.rs:662-688`) is a plain `#[test]` that hand-builds
`HeartbeatEntry { age: Duration::from_secs(120), .. }`. No store, no clock, no
fake, no sleep. That is the property to copy, and it is a consequence of the age
boundary rather than of any injection mechanism.

Rejected a `Clock` trait — which is what sleet actually has (`root.rs:31-47`) —
because it buys nothing once the age boundary exists, and it carries a real tax
sleet pays: two clocks that must be advanced in lockstep, `TestClock::advance`
for liveness and `tokio::time` for the loop (`../sleet/tests/dst.rs:93-98`).
Rejected `tokio::time::pause()` as the sole mechanism because `InMemory` stamps
`LastModified` from the real wall clock with no way to backdate it, so virtual
time cannot express "this heartbeat is 16s old" — it moves tokio's timers, not
the store's timestamps. It stays the right tool for the *scheduling* clock in
decision 6's backoff test, which is a separate concern.

Why, concretely: at 5s/15s defaults a test that genuinely waits costs 30s+ each
and goes flaky under CI load, and there are four such rules. Worse, `InMemory`
stamps `LastModified` from the real wall clock with **no way to backdate it**, so
`tokio::time::pause()` cannot express "this heartbeat is 16s old" at all — it
moves tokio's timers, not the store's timestamps. Rejected a `Clock` trait as
well: it would be this crate's first abstraction, and the pure functions that need
the time do not need an injected object to get it.

Cheap now, expensive later — it fixes the §3 signatures, so it is settled before
commit 1 rather than retrofitted through it.

### 11. Test doubles live in placement, `#[cfg(test)]` — decided

Nothing in the tree implements `ObjectStore` today (`impl ObjectStore for` has
zero hits), so decision 7 needs a `FaultStore` decorator over `InMemory` built
from scratch. It goes in a `#[cfg(test)]` module inside `crates/placement` — no
new workspace member, no shipped code, no dev-dependency cycle back to the kernel.

It scopes there cleanly because decision 7's rule lives in `liveness.rs`, and
that is the only code path a failing LIST matters to: routing and cluster tests
inject a live set directly and never need a broken store.

**`fail_list` and `fail_put` must be independently settable.** Decision 7's
partial failure — LIST throttled, PUTs still landing — is the case that motivates
withdrawing the heartbeat, and a double that only fails everything at once cannot
reproduce it. That is also the nastiest bug in this design: a node that sheds
while still publishing stays the computed owner for every peer, so they route
writes to it and it refuses every one, permanently, with nothing to time out.

Revisit only if kernel tests come to want a failing store too (index GC, matrix
publish); then it graduates to a `crates/test-support` member.

### 12. `liveness.rs`, not `directory.rs` — decided

The kernel already exports `ObjectStoreNodeDirectory` (`src/lib.rs:84`,
`engine.rs:80`), and it is the *other* operand of the intersection: static
validated cell and node ids, performing no object-store I/O whatsoever despite
the name. Naming placement's module `directory.rs` would leave two things called
directory whose relationship is `membership ∩ liveness`.

Placement's is `liveness.rs` / `LiveNodeSet`. The kernel's name is left alone —
it is the more misleading of the two, but it is a public re-export, so renaming it
is a breaking change bought for nothing here.
