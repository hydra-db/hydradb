---
title: Rendezvous placement — open questions before Phase 1
status: draft-for-review
date: 2026-07-25
branch: Turbolay-V3.5
base_commit: 4fdbbf3
tags:
  - routing
  - placement
  - single-writer
  - fencing
  - decision-record
---

# Rendezvous placement — open questions before Phase 1

Companion to `docs/plans/2026-07-25-rendezvous-placement.md`. That plan's §1 and
§2 landed in `7b0d340`; §3–§5 are blocked on the four answers below.

**How to use this file.** Each question has a `### Your answer` block at the
end. Write there — free text is fine, you do not have to pick one of the listed
options. I read this file before writing any more code, and fold the answers
back into §7 of the main plan.

## Sources — read these before changing this file

| Source | Holds |
|---|---|
| `docs/plans/2026-07-25-rendezvous-placement.md` | The plan these questions gate. §7 holds the two decisions already settled. |
| `interactive/write-routing-problem.html` | Write path traced Bolt socket → WAL commit, file:line at each step. Failure modes F-01…F-10, requirements R1–R7. |
| `interactive/write-routing-solutions.html` | TiDB/TiKV routing. OPT-1…OPT-4 scored against R1–R7. |
| memory `cell-writer-fencing-pingpong` | The prod incident: log signature, root cause, impact. |
| `../sleet/src/placement.rs`, `../sleet/src/heartbeat.rs`, `../sleet/src/root.rs:209`, `../sleet/src/daemon.rs:117-155` | The proven implementation of heartbeats, liveness filtering, and the self-always-live rule. |

---

## Facts established since the plan was written

Three things I checked in the tree, because each one changes what the right
answer is.

**1. `GRAPH_NODE_ID` is stable across restarts.** It is the pod's
`metadata.name`, injected by the StatefulSet (`node-statefulset.yaml:90-93`):

```yaml
- name: GRAPH_NODE_ID
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
```

So `graph-node-0` is still `graph-node-0` after a bounce. This matters because
rendezvous keys on the node id: had it been a per-process UUID, every restart
would have reshuffled cell ownership across the fleet and rendezvous would have
been the wrong design. It is not. Default when unset is `graph-node-0`
(`src/bin/graph_node/config.rs:108`).

**2. There is already a writer-selection mechanism, and it is part of the bug.**
`ObjectStoreBoltRoutingTableProvider::routing_table` (`routing.rs:236-241`):

```rust
let writer = reachable
    .iter()
    .find(|(node_id, _)| node_id == &self.preferred_writer_node)
    .or_else(|| reachable.first())
    .expect("reachable node list was checked")
    .1
    .clone();
```

Two problems. The `reachable.first()` fallback moves whenever a `/readyz` probe
flaps, so two clients probing at slightly different instants can name different
writers. And whatever routing decides, `ensure_local_writer` promotes on *any*
node that receives a write — routing's opinion is advisory and unenforced. That
gap is the ping-pong.

Rendezvous replaces the selection. **My proposal: keep
`with_preferred_writer_node` as an explicit override** (useful for tests and
single-node deploys, and it fails closed by validating the id is in the address
map), but have it override rendezvous rather than be the primary mechanism.
Say so in Q4's answer if you disagree.

**3. Bolt clients do not go through a k8s Service.** They connect to pod
addresses handed out by `GRAPH_BOLT_NODE_ADDRESSES`
(`charts/turbolay/templates/configmap.yaml:44`). This is the load-bearing fact
behind Q2 — see there.

---

## Q1 — The lease: what shape, if any?

### What is actually being decided

You asked for a CAS lease as the source of truth for who owns a cell's writer.
I need to correct my own earlier framing: I said a lease creates "two
authorities." That was not the real obstacle. The real obstacle is that **an
authoritative lease is not implementable against SlateDB as it stands.**

Three things I confirmed:

- **The writer epoch is readable.** `VersionedManifest::writer_epoch()` is
  public, reachable through `DbStatus.current_manifest`. It is already a
  CAS-backed, monotonic, observable lease.
- **It carries no identity.** The manifest records `epoch 18`. It never records
  `graph-node-1 holds epoch 18`. You can detect *that* you were fenced. You
  cannot learn *by whom*, or who should hold it instead. That missing identity
  is the entire gap a lease object would fill.
- **There is no fence-before-open hook.** No `expected_epoch`, no `skip_fence`,
  nothing in `Db::builder`. `build()` unconditionally claims a new epoch.

The third point is decisive. A lease object cannot *prevent* anything, because
SlateDB never consults it. It can only stop a node that already chose to check.
That is the definition of advisory, whatever we name it.

### Worked example: the prod incident, and what each option would have done

The incident from memory `cell-writer-fencing-pingpong` — three nodes, one
cell, all with the same `GRAPH_CELL_ID`:

```
t=0.00  client A → graph-node-0   write   node-0 opens Db, claims epoch 17
t=0.31  client B → graph-node-1   write   node-1 opens Db, claims epoch 18
                                          node-0's next write → Fenced
t=0.44  client C → graph-node-2   write   node-2 opens Db, claims epoch 19
                                          node-1's next write → Fenced
t=0.52  client A retries → node-0         node-0 reopens, claims epoch 20
                                          node-2's next write → Fenced
        … repeats. Every node makes progress on open and none on write.
```

What each option changes:

**Today.** node-0's log says `Fenced`. It does not say who fenced it. You are
debugging three identical logs, each of which says only "someone took it from
me." That is what the incident actually looked like.

**3a — advisory record.** After a *successful* promotion the node writes
`<base>/_cell_writers/v1/<cell>` containing `{"node_id":"graph-node-1",
"epoch":18,"at":"..."}`. Now the same incident reads:

```
node-0  WARN  fenced at epoch 17; current lease: graph-node-1 @ 18
node-1  WARN  fenced at epoch 18; current lease: graph-node-2 @ 19
```

Nothing is prevented. But the ping-pong is *visible* in one line instead of
inferred across three logs, the `NotALeader` hint carries an observed owner
rather than a computed guess, and post-incident you can answer "who actually
had it at 14:32" from object storage.

**3b — advisory + precondition.** `promote_writer` reads the lease first and
refuses if a different live node holds a `>=` epoch. In the timeline above, at
`t=0.31` node-1 would read `{graph-node-0, 17}`, see node-0 is live, and
decline. That looks like it works — but it is a read-then-act race with no
atomicity, so two nodes reading at `t=0.310` and `t=0.311` both see epoch 17
and both proceed. It converts a certainty into a probability, adds an
object-store GET to the write path, and still cannot stop a partitioned or
buggy node. I would skip it.

**3c — genuinely authoritative.** Add compare-and-set on open to SlateDB:

```rust
// hypothetical, does not exist today
let db = Db::builder(path, store)
    .expected_writer_epoch(17)   // fail if the manifest has moved past this
    .build()
    .await?;                     // -> Err(EpochMoved { current: 18 })
```

At `t=0.31` node-1's `build()` returns `Err` instead of claiming epoch 18.
node-0 keeps the writer. This is *actual* mutual exclusion, and it is the only
option here that delivers it. It is buildable — `slatedb` is our fork
(`usecortex/slatedb`, pinned at `9f4d304`), so this is an RFC against a repo we
control, not a wish. But it changes the fencing contract `architecture.md` says
everything rests on, and it belongs in its own review with its own tests.

### The options

| | What it buys | What it costs | Stops the ping-pong? |
|---|---|---|---|
| **3a** advisory record | holder identity: observability, a real `NotALeader` hint, next incident's forensics | one PUT per promotion; nothing on the write path | No — rendezvous + touch point (b) does that |
| **3b** advisory + precondition | nothing 3a doesn't | a GET on the write path, and a read-then-act race | No, and it looks like it does |
| **3c** CAS-on-open in slatedb | real mutual exclusion | RFC against our fork; changes the fencing contract; separate review | Yes, by construction |
| **none** | smallest diff | `NotALeader` carries a computed owner, not an observed one | No |

### My recommendation

**3a now, and scope 3c as a separate RFC if exclusion is the actual goal.**

Worth saying plainly, because I think it was implicit in the original ask and
should not be: **3a does not give you the mutual exclusion you were after.**
Rendezvous plus the don't-promote rule in touch point (b) is what stops the
ping-pong. The lease only records what happened. If exclusion is the goal
rather than observability, 3c is the only path, and it is better to scope it now
than to discover it after Phase 1 ships.

### Your answer

<!-- write here -->


---

## Q2 — The `/readyz` client-side fan-out: delete it?

### What is actually being decided

Not the endpoint. There are two independent consumers and I had conflated them.

**Keep, untouched** — the endpoint at `graph_node/admin.rs:48`, driven by the
k8s `readinessProbe` on the admin port (`node-statefulset.yaml:118-120`,
`indexer-deployment.yaml:96`, `multinode_k3s.sh:114`), plus
`scripts/runtime_smoke.sh:51` and the bounded wait in the Jepsen harness. None
of these touch routing.

**The question is only about** the client-side fan-out inside the routing
provider: `reachable_nodes`, `probe_node_readiness`, `replace_address_port`
(`routing.rs:152-168`, `196-218`). On every `ROUTE` request that code opens a
TCP connection to each node's admin port and asks `/readyz`.

### The catch that makes this non-obvious

Bolt clients do **not** reach nodes through a k8s Service. They connect
directly to pod addresses from `GRAPH_BOLT_NODE_ADDRESSES`
(`charts/turbolay/templates/configmap.yaml:44`). So the k8s readiness gate —
the thing you would assume protects clients — **does not gate Bolt traffic at
all.** That is precisely why `routing.rs` probes independently. Deleting the
fan-out without replacing the signal would advertise unready nodes to drivers.

### Worked example A: rolling restart (the common case)

```
t=0    kubectl rollout restart; graph-node-1 receives SIGTERM
t=0    k8s marks node-1 NotReady, removes it from the Service endpoints
       -> irrelevant: no Bolt client is using the Service
t=1    node-1's process is down; its Bolt port refuses connections
t=1    a driver holding a cached routing table dials node-1:7687 -> ECONNREFUSED
```

Today the next `ROUTE` refresh probes `/readyz`, node-1 fails, and it drops out
of the table in ~250 ms.

With readiness-gated heartbeats: node-1 **deletes its heartbeat** in the SIGTERM
handler before exiting, so the very next LIST omits it. Detection is immediate,
same as today. This is the majority of real restarts.

### Worked example B: hard crash (the regression)

```
t=0    graph-node-1 OOM-killed; SIGKILL, no shutdown hook runs
t=0    its heartbeat object still sits in the store, LastModified = t-1s
t=?    other nodes and clients keep listing it as live until it ages out
```

Today: ~250 ms, because the probe gets a TCP error.
With heartbeats: up to `heartbeat_timeout`. **This is a genuine availability
regression** and is the whole substance of Q3.

### Worked example C: up but not ready (the case only heartbeats fix)

```
t=0    graph-node-1 starts; Bolt listener is bound and accepting
t=0    AdminState.ready == false (still opening the object store, warming state)
t=1    a driver dials node-1:7687 -> connection succeeds -> query fails
```

A pure TCP-reachability check would pass here. The `/readyz` probe catches it
today. Readiness-gated heartbeat publication also catches it — node-1 simply
does not publish until `ready` flips true. Option 3 in the table below does
*not* catch it, which is why I would not do that one.

### What "readiness-gated heartbeats" means concretely

```
publish loop:   every heartbeat_interval, if AdminState.ready { PUT <base>/_graph_nodes/v1/<id> }
                                          else                { DELETE it if present }
SIGTERM:        DELETE <base>/_graph_nodes/v1/<id>, then drain
liveness:       node is live iff its object exists and now - LastModified < heartbeat_timeout
```

Liveness comes from the object's `LastModified` as reported by object storage,
never a local clock — this is sleet's rule (`../sleet/src/root.rs:147, 209`) and
it is why two nodes with skewed clocks still agree. Following sleet, **a node
always counts itself live**: if its own heartbeat looks stale it has no reliable
proof it should stop (`../sleet/src/daemon.rs:124-131`).

### The options

| | Detection: graceful | Detection: hard crash | Up-but-unready | Cost per ROUTE refresh |
|---|---|---|---|---|
| **1. Delete fan-out, gate heartbeats on ready** | immediate (DELETE) | up to `heartbeat_timeout` | caught | one LIST |
| **2. Keep fan-out, add heartbeats alongside** | ~250 ms | ~250 ms | caught | N TCP connects + one LIST |
| **3. Delete fan-out, no readiness gating** | up to timeout | up to timeout | **not caught** | one LIST |

Option 2's hidden cost is not the TCP connects, it is that you now have two
liveness signals that can disagree, and the probe result is still computed
*per-caller* — two clients asking at the same instant can get different
answers. Consistency across callers is the main reason to move to heartbeats at
all.

### My recommendation

**Option 1**, conditional on the readiness gating actually being implemented —
the deletion and the gating are one change, not two, and shipping the deletion
alone would be option 3. This folds into touch point (e) in the main plan.

### Your answer

<!-- write here -->


---

## Q3 — Heartbeat interval and timeout

### What is actually being decided

Two numbers. They set the worst-case window in which the fleet disagrees about
who is live, and therefore about who owns a cell.

sleet's defaults are 10s / 30s (`../sleet/src/config.rs:75-108`; validation
requires `interval > 0` and `interval < timeout`). If Q2 lands as option 1,
`heartbeat_timeout` becomes the *only* bound on how long a crashed node stays
in a Bolt routing table — which argues for something much tighter than 30s.

### The tension

Tighter is not free, and the cost is not the PUTs.

Rendezvous moves ownership the *instant* the live set changes, and this plan has
**no rebalance dampening** — deliberately out of scope (§6). PD has
leader-transfer scheduling for exactly this; rendezvous has nothing. So a node
that flaps in and out reclaims its cells every time it returns, and every
reclaim costs a writer open (a manifest CAS and an epoch bump).

```
graph-node-1 flaps on a ~10s period (bad node, network blips, GC pauses)

timeout = 6s:   each flap crosses the threshold
                -> ownership leaves node-1, then returns  = 2 writer opens per flap
timeout = 30s:  a 10s blip never crosses the threshold
                -> ownership never moves                  = 0 writer opens
```

That is the real trade. A 30s timeout absorbs blips; a 6s timeout reacts to
them. With Q2 option 1, though, a 30s timeout also means a genuinely dead node
is advertised to drivers for 30s.

### Cost, so it is not hand-waved

One small object PUT per node per interval, plus one LIST per routing refresh.
At S3 Standard request pricing (~$0.005 per 1,000 PUTs), for a 3-node fleet:

| interval | PUTs/day (3 nodes) | ≈ $/month |
|---|---|---|
| 2s | 129,600 | ~$19 |
| 5s | 51,840 | ~$8 |
| 10s | 25,920 | ~$4 |

Scales linearly with node count. At 3 nodes this is noise at any of these
settings; at 100 nodes on a 2s interval it is ~$650/month, so the choice should
be a config default, not a constant. **In all cases: config, with these as
defaults.** Note also that placement only ever reads the LIST result (name and
`LastModified`) — heartbeat *bodies* are observability and are never fetched on
the placement path, so body size does not affect the hot path.

### The options

| | Worst-case dead-node detection | Absorbs a 10s flap? | Convergence bound |
|---|---|---|---|
| **2s / 6s** | 6s | no | ~6s |
| **5s / 15s** | 15s | partially | ~15s |
| **10s / 30s** (sleet) | 30s | yes | ~30s |

### My recommendation

**2s / 6s**, if Q2 lands as option 1 — the deletion of the probe is what makes
detection latency matter, and 6s recovers most of the gap versus today's 250 ms.
If Q2 lands as option 2 (keep the probe), the probe already covers detection and
I would take **10s / 30s** for the flap immunity.

So this answer is partly downstream of Q2; answering Q2 may be enough.

Either way, if flapping turns out to hurt in practice, the fix is a
minimum-tenure rule (an owner keeps a cell for at least T after acquiring it),
not a longer timeout. That is noted as a follow-up in §6 and I am not building
it now.

### Your answer

<!-- write here -->


---

## Q4 — How far do I build in the next pass?

### What is actually being decided

Review size versus how soon the prod bug is actually fixed. Phase 1 is §3
(heartbeats) + §4 (four kernel touch points) + §5 (tests).

For reference, the four touch points from the main plan:

- **(a)** `ObjectStoreBoltRoutingTableProvider::routing_table`
  (`routing.rs:222-250`) — `WRITE` names the rendezvous owner; `READ`/`ROUTE`
  stay all-live. Drops the `/readyz` fan-out per Q2.
- **(b)** `RoutedGraphCluster::ensure_local_writer` (`cluster.rs:375-390`) — the
  don't-promote rule. **This is the branch that ends the duel.**
- **(c)** `GraphError::NotCellWriter { cell_id, owner }` (`core/error.rs`) mapped
  to `Neo.ClientError.Cluster.NotALeader` in `client/bolt.rs`, carrying the owner
  as a hint. HTTP gets 421 with the owner in the body.
- **(d)** Fenced-writer backoff (`core/state.rs:235-246`) — on
  `CloseReason::Fenced`, wait one `heartbeat_interval` before re-promoting.
- **(e)** `graph-node.rs` — start the heartbeat task, build the placement
  handle, pass it to the routing provider and the cluster.

### Why (b) alone is not enough

(b) stops the ping-pong, but without (c) the failure is opaque to the driver:

```
without (c):  client → graph-node-2 (not the owner)
              node-2 returns a generic error
              driver has no reason to discard its routing table
              driver retries → graph-node-2 → same error → ...

with (c):     node-2 returns Neo.ClientError.Cluster.NotALeader, owner=graph-node-0
              driver discards the routing table, re-routes, retries → graph-node-0 → ok
```

Drivers already implement this for `NotALeader`; today the Bolt module has
exactly one Neo code (`bolt.rs:1366`), so a fenced write is opaque and the
driver retries into the same wrong node. (c) is small and it is what turns a
correct refusal into a working client.

### The test that matters

§5's last row, and it is the reason to go all the way:

> **3 promotable nodes, 1 cell, concurrent writes → exactly one epoch bump.**

That is the prod incident, reproduced as a test. It cannot be written until
(a), (b) and (e) all exist.

### The options

| | What lands | What prod looks like after |
|---|---|---|
| **All of Phase 1**, staged commits | §3, (a)–(e), §5 including the regression test | ping-pong fixed, drivers re-route correctly, regression guarded |
| **Crate only** (§3) | `heartbeat.rs`, `directory.rs`, tested against `InMemory` | unchanged — ping-pong continues |
| **Crate + (b)** | §3 plus the don't-promote rule | ping-pong stops; non-owner writes fail opaquely and drivers retry into the same wrong node |

Staged commits in the first option would be roughly: §3 crate → (a) routing →
(b)+(c) refusal and mapping → (d) backoff → (e) wiring → §5 regression test.
Each stands alone and is separately revertable.

### My recommendation

**All of Phase 1, staged commits.** The crate on its own changes nothing in
prod, and `crate + (b)` fixes the duel while leaving clients unable to recover
from it. The full set is a bigger review but the commits are individually small
and the last one is the incident reproduced.

### Your answer

<!-- write here -->


---

## After you answer

I fold the four answers and their rationale into §7 of
`docs/plans/2026-07-25-rendezvous-placement.md`, update `status:` there, and
start on §3. This file stays as the decision record.
