---
title: Rendezvous placement for cell writers
status: step-2-complete
date: 2026-07-25
branch: Turbolay-V3.5
base_commit: 3bacd71
head_commit: 7b0d340
tags:
  - routing
  - placement
  - single-writer
  - fencing
---

# Rendezvous placement for cell writers — code plan

Sections 1 and 2 landed in `7b0d340`. Sections 3–5 are unstarted; section 3 is
blocked on decision 4 in §7.

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
    heartbeat.rs   ⬜ write, list, parse, liveness — blocked on decision 4
    directory.rs   ⬜ live set = configured membership ∩ fresh heartbeats
```

## 2. The frozen hash — LANDED

`crates/placement/src/hash.rs`. Copied from `sleet/src/placement.rs`, which is
the proven version of exactly this, with sleet's `(database, service)` pair
replaced by `(scope, cell)` — sleet's `Service` enum and its `mirror` triple
variant (`score_target`/`rank_target`/`owner_target`) have no analogue here and
were dropped. FNV-1a 64, key encoding and tie-break frozen as a wire format:

```
score(scope, cell, node) = fnv1a64(scope ‖ 0x00 ‖ cell_id ‖ 0x00 ‖ node_id)
rank                     = descending by score, ties broken by node_id ascending
owner                    = rank[0], or None when no node is live
```

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

Seven tests, all passing. The pinned values, which are now a compatibility
surface:

| `scope` | cell | node | score |
|---|---|---|---|
| `acme/graphs/social` | `cell-a` | `graph-node-0` | `0xf0dc2317deb297c5` |
| `acme/graphs/social` | `cell-a` | `graph-node-1` | `0xf0dc2217deb29612` |
| `acme/graphs/social` | `cell-b` | `graph-node-0` | `0x39ffe3dd3cc79ffe` |
| `acme/graphs/other` | `cell-a` | `graph-node-0` | `0xc15e57802a479f04` |

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

Following sleet: **a node always counts itself live.** If its own heartbeat looks
stale it has no reliable proof it should stop.

Defaults: `heartbeat_interval` 10s, `heartbeat_timeout` 30s. Convergence after a
node dies is therefore bounded by ~`heartbeat_timeout`. Decision 4 in §7 argues
for tightening these to ~2s/6s, since heartbeat freshness would then be the
only readiness signal Bolt clients have.

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
before re-promoting. Combined with (b), mutual fencing is then bounded by view
convergence rather than by nothing.

**e. `graph-node.rs`** — start the heartbeat task; build the placement handle
from `GRAPH_NODE_ID` and the node directory; pass it to the routing provider and
the cluster.

## 5. Tests

| Test | Where | Guards |
|---|---|---|
| Golden hash values, pinned hex | `turbolay-placement` | the wire format |
| Remove a node, order of the rest preserved | `turbolay-placement` | minimal disruption |
| Heartbeat expiry, self-always-live | `turbolay-placement` | liveness rules |
| Routing table names the rendezvous owner for WRITE | `bolt/routing` tests | touch point (a) |
| Non-owner write returns `NotCellWriter`, not a promotion | `cluster` tests | touch point (b) |
| **3 promotable nodes, 1 cell, concurrent writes → exactly one epoch bump** | integration | **the prod regression** |

The last row is the one that matters — it is the incident, reproduced.

## 6. Explicitly out of scope

- **CAS lease objects.** Superseded by decision 3 in §7 — the argument there is
  sharper than "two authorities" and rests on what SlateDB actually exposes. An
  advisory record (3a) is in scope for the crate; an authoritative one (3c) is
  a separate RFC against our `slatedb` fork.
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

### 3. Lease — STILL OPEN

Correcting the framing in §6 below: the problem is not "two authorities", it is
that **an authoritative lease is not currently implementable**. Three things
confirmed against the tree:

- The writer epoch **is** readable — `VersionedManifest::writer_epoch()` is
  public via `DbStatus.current_manifest`. It is already a CAS-backed,
  monotonic, observable lease.
- It carries **no identity**. The manifest records "epoch 18", never "node-1
  holds epoch 18". You can detect that you were fenced; not by whom. That
  missing identity is the entire gap a lease object would fill.
- There is **no fence-before-open hook** — no `expected_epoch`, no
  `skip_fence`, nothing in `Db::builder`. `build()` unconditionally claims a
  new epoch.

The third point is decisive: a lease object cannot *prevent* anything, because
SlateDB never consults it. It can only stop a node that already chose to check.
That is advisory, whatever we call it.

| | What it buys | Cost |
|---|---|---|
| **3a** advisory record — write `{node_id, epoch}` after a successful promotion | the missing identity: observability, the `NotALeader` hint, the next incident's debugging | none on the write path; epoch stays sole authority |
| **3b** advisory + precondition — `promote_writer` reads the lease and refuses if a different live node holds a ≥ epoch | nothing 3a doesn't | an object-store read on the write path, and it *still* cannot stop a buggy or partitioned node — worst of both |
| **3c** genuinely authoritative — add `expected_writer_epoch` CAS-on-open to SlateDB, so `build()` fails instead of fencing | real mutual exclusion | changes the fencing contract `architecture.md` rests on; needs its own review and tests |

Recommendation: **3a now, 3c as a separate RFC.** 3c is buildable —
`slatedb` is our fork (`usecortex/slatedb`, pinned at `9f4d304`), so it is an
RFC against a repo we control, not a wish.

Said plainly: **3a does not deliver mutual exclusion.** Rendezvous plus the
don't-promote rule in touch point (b) is what stops the ping-pong; the lease
only records what happened. If exclusion is the actual goal, 3c is the only
path and should be scoped now rather than discovered later.

### 4. Deleting the `/readyz` probe path — STILL OPEN

There are two independent consumers, previously conflated here.

**Keep, untouched** — the endpoint at `graph_node/admin.rs:48`, driven by the
k8s `readinessProbe` on the admin port (`node-statefulset.yaml:118-120`,
`indexer-deployment.yaml:96`, `multinode_k3s.sh:114`), plus
`scripts/runtime_smoke.sh:51` and the bounded wait in the Jepsen harness. None
of these touch routing.

**Delete** — only the client-side fan-out inside the routing provider:
`reachable_nodes`, `probe_node_readiness`, `replace_address_port`
(`routing.rs:152-168, 196-218`).

The catch: Bolt clients do **not** reach nodes through a k8s Service. They
connect directly to pod addresses handed out by `GRAPH_BOLT_NODE_ADDRESSES`
(`charts/turbolay/templates/configmap.yaml:44`), so k8s readiness does not gate
Bolt traffic at all. That is precisely why `routing.rs` probes independently.
Deleting the probe without replacing the signal would advertise unready nodes
to drivers.

The replacement, which must not be skipped and folds into touch point (e): a
node publishes its heartbeat **only while `AdminState.ready` is true**, and
deletes it on graceful shutdown or when it goes unready. Readiness then
propagates through the same channel as liveness.

One genuine regression: detecting a hard-crashed node goes from ~250 ms to up
to `heartbeat_timeout`. Graceful cases stay immediate (heartbeat deleted).
Since a heartbeat is one small object per node, tightening the §3 defaults from
sleet's 10s/30s to roughly **2s interval / 6s timeout** buys most of that back.

Recommendation: yes, delete the fan-out — **conditional** on moving readiness
into heartbeat publication.
