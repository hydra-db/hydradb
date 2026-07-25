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

**Status: Q1, Q2 and Q3 settled. Q4 still open.**

| | Question | Answer |
|---|---|---|
| Q1 | Lease shape | **3a** — advisory record, observability only. 3c dropped; sleet shows it is not needed and neither slatedb tree can reach it. |
| Q2 | Delete the `/readyz` fan-out | **Option 1** — delete the client-side fan-out, gate heartbeat publication on `AdminState.ready`. One change, not two. |
| Q3 | Heartbeat interval / timeout | **5s / 15s**, config with validation |
| Q4 | Build scope for the next pass | open |

Two smaller things surfaced by Q1 and Q2 and not yet agreed, carried at the end
of this file: **what a failed node LIST does to ownership**, and **whether
`with_preferred_writer_node` survives**.

**How to use this file.** Each question has a `### Your answer` block at the
end. Write there — free text is fine, you do not have to pick one of the listed
options. I read this file before writing any more code, and fold the answers
back into §7 of the main plan.

## Sources — read these before changing this file

| Source                                                                                                                 | Holds                                                                                                            |
| ---------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `docs/plans/2026-07-25-rendezvous-placement.md`                                                                        | The plan these questions gate. §7 holds the two decisions already settled.                                       |
| `interactive/write-routing-problem.html`                                                                               | Write path traced Bolt socket → WAL commit, file:line at each step. Failure modes F-01…F-10, requirements R1–R7. |
| `interactive/write-routing-solutions.html`                                                                             | TiDB/TiKV routing. OPT-1…OPT-4 scored against R1–R7.                                                             |
| memory`cell-writer-fencing-pingpong`                                                                                   | The prod incident: log signature, root cause, impact.                                                            |
| `../sleet/src/placement.rs`, `../sleet/src/heartbeat.rs`, `../sleet/src/root.rs:209`, `../sleet/src/daemon.rs:117-155` | The proven implementation of heartbeats, liveness filtering, and the self-always-live rule.                      |

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
writers. And whatever routing decides, `ensure_local_writer` promotes on _any_
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

## How sleet actually does it

Added after you asked "is 3(c) how sleet does it?". Short answer: **no, and it
is close to the opposite design.** This section is the evidence; Q1 below is
finalised on it.

### Sleet never opens a `Db` at all

There is not one `Db::builder`, `Db::open` or `DbBuilder` in sleet's `src/`.
Every hit is in `tests/`. The only handle sleet constructs is
`slatedb::admin::Admin` (`../sleet/src/services.rs:71-91`):

```rust
pub fn from_parts(url: &str, store: Arc<dyn ObjectStore>, path: StorePath) -> Self {
    let admin = AdminBuilder::new(path.clone(), store.clone()).build();
    Self { url: url.to_string(), admin, store, path }
}
```

So sleet is never a writer and never takes a `writer_epoch`. Its analogous
problem is `compactor_epoch` ping-pong between two compaction coordinators —
same shape, different epoch. Everything below is about that.

Note also that `DatabaseHandle::open` takes only a URL. It has no node
identity and no placement argument, so **it could not check ownership even in
principle.** Ownership is checked somewhere else entirely.

### There is no lease object in sleet. None.

Sleet stores exactly three things under the fleet root: `sleet.toml` and
`dbs/<db>.toml` (operator intent), and `nodes/<id>.<letters>.json` (liveness
plus offered roles). The heartbeat is an **unconditional PUT** — no `PutMode`,
no ETag, no `UpdateVersion` (`../sleet/src/daemon.rs:292`). It says "I am alive
and I offer these services." It never says "I own database X."

The only conditional writes in the whole codebase are `PutMode::Create` for
`sleet register` (guarding a config file) and for mirror manifest commits.
There is no compare-and-swap on ownership anywhere.

**Sleet stores liveness and intent. It recomputes ownership from scratch every
tick.** That is the entire model.

### What a non-owner does

`owned_assignments` (`../sleet/src/daemon.rs:117-162`) is a pure function —
heartbeat entries in, a map of owned assignments out, no I/O. Non-ownership is
just absence of a key:

```rust
let owners = placement::owners(url, service, count, &candidates);
if owners.contains(&node_id) {
    owned.insert((url.clone(), service, None), resolved.clone());
}
```

`reconcile` then spawns only for keys present in the map, and cancels any
running task whose key is absent (`daemon.rs:327-353`). The check happens
**at task-spawn time only** — not at open, not per operation.

### The fence handler — this is the part we need to copy exactly

`../sleet/src/daemon.rs:458-472`:

```rust
let delay = match result {
    Ok(()) => break,
    Err(e) if e.is_fenced() => {
        info!(database = %url, "coordinator fenced; retrying after one heartbeat interval");
        backoff = Duration::from_secs(1);
        heartbeat_interval
    }
    Err(e) => {
        warn!(database = %url, service = service.as_str(), "task failed: {e}");
        backoff = (backoff * 2).min(MAX_BACKOFF);
        backoff
    }
};
```

Three things here that our plan's touch point (d) currently gets wrong or
leaves vague:

1. A fence **resets** `backoff` to 1s rather than advancing the exponential
   ladder. A fence is treated as *view skew*, not as a failure. Our (d) says
   "wait one `heartbeat_interval`" but does not say it must not feed the
   exponential path — and if it did, a fenced node would back off to minutes.
2. The wait is exactly one `heartbeat_interval`, and the reason is stated in
   the doc comment at `daemon.rs:391-394`: it gives *the rival* time to refresh
   its view and stand down. It is not a politeness delay, it is sized to the
   convergence period.
3. The retry is **unconditional** — it does not re-check ownership. "Rerun, and
   let the outer loop cancel me if I was wrong." Sleet accepts that a fenced
   node will re-fence the winner once more before converging.

### Double-running is explicitly permitted, not prevented

This is the design's central claim, and it is stated four times.

`../sleet/rfcs/0001-design.md:35-37`, Non-goals:

> Sleet does not add a lock service. SlateDB's manifest CAS, epochs, and
> compaction job claims remain the mutual exclusion mechanisms.

`rfcs/0001-design.md:251-260`, Failure behavior:

> Sleet treats placement as an efficiency decision. Safety belongs to SlateDB.

`rfcs/0001-design.md:275-284`, Coordinator fencing — the direct ping-pong
answer:

> A running coordinator can be fenced by a newer `compactor_epoch`. Sleet
> treats that as evidence of view skew. The fenced task waits one
> `heartbeat_interval`, then reruns. […] **Mutual fencing can last only while
> the two nodes disagree about ownership, which is bounded by `config_poll`
> plus one heartbeat tick. The cost is a short compaction stall.**

And `../sleet/src/daemon.rs:10-14`:

```rust
//! Assignment is an efficiency mechanism only: every failure mode here
//! at worst double-runs a service, which SlateDB's fencing and CAS
//! claims make safe.
```

Sleet even **counts itself live when its own heartbeat looks stale**
(`daemon.rs:124-130`), deliberately choosing overlap over gaps: "peers that
consider it dead take over in parallel, which is a safe double-run, whereas
excluding itself would leave the share unowned."

### It is model-checked, and the model asserts the ping-pong is reachable

`../sleet/specs/coordination.fizz:167-175`:

```
# The design's stated worst case, a transient double-run, is
# reachable in the model.
exists assertion DoubleRunReachable:
    return len([db for db in DBS if len(runners[db]) > 1]) > 0
```

Paired with a `Converged` liveness property (an `eventually always` requiring
`runners[db] == [expected]` and `len(fenced[db]) == 0` after churn stops) whose
stated purpose is to catch exactly the failure we care about — the spec header
notes it "checks convergence and the absence of fence livelock in one property
(a livelock would flap runners/fenced forever and violate eventually-always)."

So sleet does not claim the duel cannot happen. It claims the duel is bounded,
and it proves the bound.

### Two hazards in sleet worth deciding on before we copy it

- **A LIST failure freezes ownership rather than shedding it**
  (`daemon.rs:243`): `Err(e) => warn!("failed to LIST nodes/: {e}; keeping
  current assignments")`. A node partitioned from the object store keeps
  running everything it had, indefinitely, while its peers time it out and take
  over. For sleet that is a safe double-run. For a *writer* it is a
  guaranteed epoch duel with no convergence, because the partitioned node's
  view can never update. **We need a different answer here than sleet's.**
- **The operator CLI bypasses placement entirely** (`../sleet/src/ops.rs:263`,
  `:406`, and the `sleet mirror sync` one-shots). Our analogue is any admin or
  HTTP path that can reach `ensure_local_writer` without going through
  rendezvous.

### Side-by-side

| | sleet | this plan |
|---|---|---|
| Placement | rendezvous, recomputed each tick | same (`crates/placement`) |
| Ownership stored? | no | Q1 |
| Checked where | task spawn, in the supervisor | `ensure_local_writer`, per promotion — **stricter than sleet** |
| On fence | reset backoff to 1s, wait one interval, retry unconditionally | touch point (d) — should match exactly |
| Mutual exclusion | delegated to slatedb epochs; duel bounded, not prevented | same |
| Duel bound | `config_poll` + one heartbeat tick | `heartbeat_timeout` |
| Verified by | Fizz model, `Converged` liveness property | §5 integration test — see Q4 |

The one place we are *stricter* than sleet: sleet checks ownership once at
spawn and never again, so a task keeps running through an ownership change
until the next reconcile tick. Our touch point (b) checks on **every**
promotion. That is a better guarantee and costs nothing, because the live set
is already in memory.

---

## Q1 — The lease: what shape, if any? — **FINALISED**

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
  `graph-node-1 holds epoch 18`. You can detect _that_ you were fenced. You
  cannot learn _by whom_, or who should hold it instead. That missing identity
  is the entire gap a lease object would fill.
- **There is no fence-before-open hook.** No `expected_epoch`, no `skip_fence`,
  nothing in `Db::builder`. `build()` unconditionally claims a new epoch.

The third point is decisive. A lease object cannot _prevent_ anything, because
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

**3a — advisory record.** After a _successful_ promotion the node writes
`<base>/_cell_writers/v1/<cell>` containing `{"node_id":"graph-node-1", "epoch":18,"at":"..."}`. Now the same incident reads:

```
node-0  WARN  fenced at epoch 17; current lease: graph-node-1 @ 18
node-1  WARN  fenced at epoch 18; current lease: graph-node-2 @ 19
```

Nothing is prevented. But the ping-pong is _visible_ in one line instead of
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
node-0 keeps the writer. This is _actual_ mutual exclusion, and it is the only
option here that delivers it. It is buildable — `slatedb` is our fork
(`usecortex/slatedb`, pinned at `9f4d304`), so this is an RFC against a repo we
control, not a wish. But it changes the fencing contract `architecture.md` says
everything rests on, and it belongs in its own review with its own tests.

{user_note}: slatedb is our fork (usecortex/slatedb, pinned at 9f4d304) -> no! use the slatedb original , version 0.14.1 pls -- https://github.com/slatedb/slatedb , tell me if that works? 0.14.1 also has a distributed compaction (but more on that later. just note that.)

**Answer: no, 0.14.1 does not work — and it would not help Q1 even if it did.**

I checked both trees on disk. The fork is at
`~/.cargo/git/checkouts/slatedb-c41af1fe6068aba3/9f4d304`, upstream 0.14.1 at
`~/.cargo/registry/src/index.crates.io-*/slatedb-0.14.1` (sleet pulls it, so it
is already local).

**1. The fork is 0.14.1 + 43 upstream commits + 8 local ones.** Its
`version` field says `0.14.1`, but it is not that tree — its base `c8e62bc` is
43 commits past the release tag. Switching is a **51-commit downgrade**, not a
sideways move. Among what we would give up: `#1900`, "re-establish `DbReader`
checkpoint if GC removed it" — a correctness fix for exactly our reader path.

**2. Six APIs we use do not exist upstream**, all in the durable-reader path:

| API | turbolay call sites |
|---|---|
| `DbReaderSnapshot` (type does not exist upstream at all) | `core/state.rs:8, :96` |
| `DbReader::snapshot()` | `core/state.rs:451, :458, :467, :474` |
| `DbReaderSnapshot::{seq, last_wal_id, get_with_options, scan_prefix_with_options}` | `core/state.rs:105-150, :511` |
| `DbReader::refresh()` | `core/state.rs:338, :498` |
| `Db::last_flushed_wal_id()` | `core/state.rs:514` |
| `ErrorKind::DatabaseMissing` | `core/state.rs:306` |

`GraphStorageSnapshot::Reader(Arc<DbReaderSnapshot>)` has no upstream
equivalent, so follower nodes would fall back to unpinned `DbReader::get/scan`
— **losing snapshot isolation for every query served by a follower**, which
contradicts `architecture.md:168` and `README.md:23`. That is a design change,
not a dependency swap. `index_store.rs:112-134` also stamps every index
generation off `snapshot.last_wal_id()`, and two open bugs
(`BFG-011-wal-tail-visibility-hole.md:44`, `BFG-009-explainer.html:416`) are
*about* that frontier — the fork API is load-bearing in unresolved correctness
work.

**3. On distributed compaction: 0.14.1 has it, and so does the fork.** Same
file set — `compaction_worker.rs`, `compactor.rs`, `compactor_executor.rs`,
`compactions_store.rs`, `subcompaction.rs` — exporting `CompactionWorker`,
`CompactionWorkerBuilder`, `CompactorBuilder`, `VersionedCompactions`. **Nothing
is gained by switching and nothing is lost by staying.** Noted for later as you
asked; it is available to us today.

**4. On fencing, which is what Q1 is about: 0.14.1 gives us nothing the fork
lacks.** Neither tree has `expected_writer_epoch`, `skip_fence`, or any
epoch/read-only option on `DbBuilder` — I grepped both. `build()` claims a new
epoch unconditionally in both. `VersionedManifest::writer_epoch()`, `DbStatus`,
`CloseReason::Fenced` and `WalReader::new` are identical. So switching would
*remove* our ability to land a 3c-style change ourselves, and gain no fencing
capability at all.

**Two things that are genuinely non-issues**, worth stating because they are the
usual blockers: both trees pin `object_store = "0.14.0"`, so
`Arc<dyn ObjectStore>` unifies either way; and `default-features = false,
features = ["aws", "foyer"]` works verbatim on 0.14.1.

**If the goal is "stop depending on a fork"** — a reasonable goal, and no commit
message ever recorded why the fork was adopted — the cheap path is to
**upstream the 8 local commits** (+460 lines across 5 files, and upstream's own
`DbReaderMode` work in `#1915` shows they are moving the same direction), then
pin a future release. Downgrading to 0.14.1 is the expensive path to the same
place. Happy to scope that as its own piece of work; it is independent of this
plan.

### The options

|                                | What it buys                                                                       | What it costs                                                       | Stops the ping-pong?                        |
| ------------------------------ | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------- | ------------------------------------------- |
| **3a** advisory record         | holder identity: observability, a real`NotALeader` hint, next incident's forensics | one PUT per promotion; nothing on the write path                    | No — rendezvous + touch point (b) does that |
| **3b** advisory + precondition | nothing 3a doesn't                                                                 | a GET on the write path, and a read-then-act race                   | No, and it looks like it does               |
| **3c** CAS-on-open in slatedb  | real mutual exclusion                                                              | RFC against our fork; changes the fencing contract; separate review | Yes, by construction                        |
| **none**                       | smallest diff                                                                      | `NotALeader` carries a computed owner, not an observed one          | No                                          |

### Your answer

> is 3(c) how sleet does it? we do want to avoid ping pong

**No. Sleet does none of 3a, 3b or 3c** — see the section above. It has no
lease object, no CAS-on-open, and it never checks an epoch before opening. It
recomputes ownership from a frozen hash every tick, declines to start when it
is not the owner, and on a fence sleeps exactly one heartbeat interval and
retries. It then *asserts in a model checker that the double-run is reachable*
and proves only that it converges.

So the answer to "we do want to avoid ping pong" is not the lease. It is:

| | mechanism | in our plan |
|---|---|---|
| stops nodes from racing in the first place | deterministic rendezvous + decline if not owner | §2 (landed) + touch point **(b)** |
| bounds the duel when views briefly disagree | fence → reset backoff, wait one interval, retry | touch point **(d)** |
| makes the duel *visible* when it happens | an advisory record of who holds what | this question |

**(b) and (d) are the fix. The lease is observability.** They are independent,
and this plan already contains both.

### Finalised: 3a — advisory record, scoped to observability. 3c dropped.

Three reasons the sleet investigation moved this from "3a, and scope 3c" to
"3a, and drop 3c":

1. **3c is not required to avoid the ping-pong.** Sleet runs the same
   architecture at fleet scale with no CAS-on-open and converges; the bound is
   `config_poll` + one heartbeat tick, and it is model-checked. Our design is
   *stricter* than sleet's — we check ownership on every promotion, sleet checks
   once at task spawn — so our bound is no worse.
2. **3c cannot be reached from either dependency.** Neither the fork nor
   upstream 0.14.1 has any epoch option on `DbBuilder`. It is a real upstream
   feature request, not a config change, and it would gate Phase 1 on a
   dependency change. Not worth it for a bound we already have.
3. **The prod incident's actual pain was diagnostic, not correctness.** Three
   nodes logging an identical `Fenced` with no attribution. 3a fixes precisely
   that, and nothing else claims to.

**What 3a is, concretely.** After a *successful* promotion, and only then:

```
PUT <base>/_cell_writers/v1/<cell_id>
    {"node_id":"graph-node-1","epoch":18,"at":"2026-07-25T14:32:07Z"}
```

Read on two paths only, both off the write path: when logging a
`CloseReason::Fenced`, and when building the `NotCellWriter { owner }` hint in
touch point (c). Never consulted to decide whether to promote — that is
rendezvous' job, and consulting it would be 3b, which I still argue against
(read-then-act race, a GET on the write path, and it looks like it works).

**Explicitly recorded so nobody re-reads this as a guarantee:** the SlateDB
writer epoch remains the only authority. The record can be stale — a node can
be fenced a millisecond after writing it. It answers "who last successfully
promoted", not "who holds the writer right now". If it ever disagrees with the
manifest, the manifest is right.

**One thing to copy from sleet that our plan currently under-specifies.** Touch
point (d) says "wait one `heartbeat_interval` before re-promoting". Sleet's
version also **resets the exponential backoff to 1s** on a fence, because a
fence is view skew rather than a failure (`daemon.rs:461-464`). Without that,
repeated fences ride the exponential ladder to `MAX_BACKOFF` and a node that is
merely converging looks dead. I will match sleet exactly.

**And one place we must *not* copy sleet.** On a failed LIST of the node
directory, sleet keeps its current assignments (`daemon.rs:243`). For a
compaction coordinator that is a safe double-run. For a *writer* it is a
guaranteed unbounded duel: a node partitioned from the object store can never
learn it lost ownership, so it re-promotes forever against a node that thinks
it won. My proposal is that a **LIST failure sheds ownership** — if we cannot
read the live set we cannot claim to own anything, so refuse promotion and
return `NotCellWriter` with no owner hint. That trades availability for the
bound, which is the right trade for the writer path specifically. Flag it in
Q4's answer if you disagree.

### Follow-up this opens (not for now)

Sleet's convergence claim is backed by `../sleet/specs/coordination.fizz` — a
`Converged` liveness property specifically designed to catch fence livelock.
This repo already has `quint-models/` and prior formal-methods work, so the
same property is expressible here. §5's integration test proves one scenario;
a liveness spec would prove the class. Worth doing after Phase 1 lands, not
before.

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
_not_ catch it, which is why I would not do that one.

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

|                                                 | Detection: graceful | Detection: hard crash    | Up-but-unready | Cost per ROUTE refresh    |
| ----------------------------------------------- | ------------------- | ------------------------ | -------------- | ------------------------- |
| **1. Delete fan-out, gate heartbeats on ready** | immediate (DELETE)  | up to`heartbeat_timeout` | caught         | one LIST                  |
| **2. Keep fan-out, add heartbeats alongside**   | ~250 ms             | ~250 ms                  | caught         | N TCP connects + one LIST |
| **3. Delete fan-out, no readiness gating**      | up to timeout       | up to timeout            | **not caught** | one LIST                  |

Option 2's hidden cost is not the TCP connects, it is that you now have two
liveness signals that can disagree, and the probe result is still computed
_per-caller_ — two clients asking at the same instant can get different
answers. Consistency across callers is the main reason to move to heartbeats at
all.

### My recommendation

**Option 1**, conditional on the readiness gating actually being implemented —
the deletion and the gating are one change, not two, and shipping the deletion
alone would be option 3. This folds into touch point (e) in the main plan.

### Your answer

> can you explain this more? in simple language?
>
> yes

**Decided: option 1.** Delete the client-side fan-out; gate heartbeat
publication on readiness. The plain-language version of what that means,
recorded here so the next reader does not have to reconstruct it from the
options table.

**What exists today.** Every time a Bolt driver asks "which nodes can I talk
to?", the node answering opens a TCP connection to *every* node's admin port and
calls `/readyz`. Three nodes, three connects, on every routing refresh. The
answer is then built from whoever replied.

Two problems with that. It is computed fresh **per caller**, so two drivers
asking at the same instant can get different answers when a probe flaps — there
is no shared truth about who is alive. And it is N connects per refresh, forever,
to learn something that barely changes.

**What replaces it.** Every node writes a small object every
`heartbeat_interval`:

```
<base>/_graph_nodes/v1/graph-node-0
<base>/_graph_nodes/v1/graph-node-1
<base>/_graph_nodes/v1/graph-node-2
```

Alive means *your object exists and object storage reports `LastModified`
younger than `heartbeat_timeout`*. Anyone needing the live set does **one LIST**
and no connects. Everyone LISTing at the same instant sees the same bytes, and
that is the actual requirement: rendezvous only works if every node computes
ownership from the *same* live set. A per-caller probe cannot give that.

**The catch.** You would assume Kubernetes readiness already protects clients.
It does not. Bolt drivers connect straight to pod addresses from
`GRAPH_BOLT_NODE_ADDRESSES`, never through a k8s Service, so the
`readinessProbe` gates nothing for them. That is why `routing.rs` grew its own
probe in the first place. Delete the probe and put nothing in its place and we
start handing drivers the address of a node that has booted but cannot serve.

**So it is one change, not two:** a node publishes its heartbeat only while it
is ready, and deletes it the moment it is not.

```
every heartbeat_interval:   ready?  -> PUT    <base>/_graph_nodes/v1/<id>
                            not?    -> DELETE <base>/_graph_nodes/v1/<id>
SIGTERM:                            -> DELETE <base>/_graph_nodes/v1/<id>, then drain
```

Readiness and liveness now travel through the same channel. One signal instead
of two that can disagree.

**The three cases, including the one that gets worse:**

| | today | after |
|---|---|---|
| Rolling restart (the common case) | ~250 ms | **immediate** — the node deletes its own heartbeat before exiting |
| Hard crash / OOM-kill | ~250 ms | **up to 15s** — nothing deleted the object, it has to age out |
| Booted but not ready yet | caught by the probe | caught — it never published |

Row two is a real availability regression and is not being dressed up: a
SIGKILLed node stays in routing tables for up to `heartbeat_timeout`, which Q3
fixes at 15s. What it buys is one consistent live set instead of N per-caller
probe results.

**The wrong version of this change** is deleting the probe *without* the
readiness gating — that is option 3, and it breaks the third row too. Deletion
and gating ship together or not at all. Folds into touch point (e).

Unchanged by this: the `/readyz` endpoint at `graph_node/admin.rs:48` and all of
its non-routing consumers — the k8s `readinessProbe`, `scripts/runtime_smoke.sh:51`,
the Jepsen harness wait. Only `reachable_nodes`, `probe_node_readiness` and
`replace_address_port` are deleted.

---

## Q3 — Heartbeat interval and timeout

### What is actually being decided

Two numbers. They set the worst-case window in which the fleet disagrees about
who is live, and therefore about who owns a cell.

sleet's defaults are 10s / 30s (`../sleet/src/config.rs:75-108`; validation
requires `interval > 0` and `interval < timeout`). If Q2 lands as option 1,
`heartbeat_timeout` becomes the _only_ bound on how long a crashed node stays
in a Bolt routing table — which argues for something much tighter than 30s.

### The tension

Tighter is not free, and the cost is not the PUTs.

Rendezvous moves ownership the _instant_ the live set changes, and this plan has
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
| -------- | ------------------ | --------- |
| 2s       | 129,600            | ~$19      |
| 5s       | 51,840             | ~$8       |
| 10s      | 25,920             | ~$4       |

Scales linearly with node count. At 3 nodes this is noise at any of these
settings; at 100 nodes on a 2s interval it is ~$650/month, so the choice should
be a config default, not a constant. **In all cases: config, with these as
defaults.** Note also that placement only ever reads the LIST result (name and
`LastModified`) — heartbeat _bodies_ are observability and are never fetched on
the placement path, so body size does not affect the hot path.

### The options

|                       | Worst-case dead-node detection | Absorbs a 10s flap? | Convergence bound |
| --------------------- | ------------------------------ | ------------------- | ----------------- |
| **2s / 6s**           | 6s                             | no                  | ~6s               |
| **5s / 15s**          | 15s                            | partially           | ~15s              |
| **10s / 30s** (sleet) | 30s                            | yes                 | ~30s              |

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

{user_note} you can use 5 / 15s pelase

**Decided: `heartbeat_interval` 5s, `heartbeat_timeout` 15s.** Config, with
these as defaults, and validated like sleet does (`interval > 0` and
`interval < timeout`, `../sleet/src/config.rs:1018-1024`) so a misconfiguration
that would make every node permanently dead is rejected at startup rather than
in production.

What these numbers now bind, in one place:

| | value |
|---|---|
| worst-case dead-node detection (hard crash) | 15s |
| graceful shutdown detection | immediate, via heartbeat DELETE |
| placement convergence after a node dies | ~15s |
| fence backoff in touch point (d) | 5s — one interval, per sleet |
| heartbeat PUT cost, 3 nodes | ~52k/day, ~$8/month |

The 15s timeout partially absorbs a ~10s flap, which is the case 6s would have
churned on. If flapping still hurts in practice the fix is a minimum-tenure
rule (an owner keeps a cell for at least T after acquiring it), not a longer
timeout — noted as a follow-up in §6 of the main plan, not built now.

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

### Q2 changed the shape of this question

Answering Q2 as option 1 coupled two touch points that the main plan lists as
independent. **(a) deletes the probe fan-out, so (a) cannot function until
something publishes heartbeats — and publishing is part of (e).** The staging in
the main plan puts (e) last. That order is now wrong.

This is not a cosmetic reorder. It is a **rolling-upgrade hazard**, and it is
the one thing here that can make prod briefly worse rather than better:

```
rollout in progress, 3 nodes, node-0 upgraded first

node-0 (new):  publishes a heartbeat, LISTs _graph_nodes/v1/, sees ONE node
               -> rendezvous says: node-0 owns every cell
               -> promotes, claims the epoch
node-1 (old):  still probing /readyz, still using preferred_writer_node
               -> still promotes on any write it receives
node-2 (old):  same
```

The new node is not wrong — it computed correctly over the live set it could
see. The live set was just incomplete, because old nodes do not publish. During
that window the duel continues exactly as today, and the "exactly one epoch
bump" property does not hold until every node is upgraded.

The fix is ordering, not code: **publish before anyone consumes.** If the
heartbeat publisher ships in an earlier commit than the routing change, then by
the time (a) lands every node is already publishing, and the first node to read
the live set reads a complete one.

### The staging, revised

Seven commits, each separately revertable, each leaving the tree green:

| # | Commit | Runtime effect on prod | Risk |
|---|---|---|---|
| 1 | §3 crate — `heartbeat.rs`, `directory.rs`, tested against `InMemory` | none — nothing calls it | none |
| 2 | **publisher** — heartbeat task in `graph-node.rs`, gated on `AdminState.ready`, DELETE on SIGTERM | writes one small object per node per 5s; **nothing reads it yet** | near-zero, and independently observable in the bucket before anything depends on it |
| 3 | **(a)** routing provider — live set from LIST, `WRITE` names the rendezvous owner, delete the fan-out | first client-visible change: writes are steered to one node | medium — the routing table changes shape |
| 4 | **(b)+(c)** don't-promote rule + `NotCellWriter` → `Neo.ClientError.Cluster.NotALeader` | **the duel stops**; drivers re-route on refusal | medium — this is the behaviour change that matters |
| 5 | **(d)** fence backoff: wait one interval, reset backoff to 1s | bounds mutual fencing during view skew | low |
| 6 | **3a** advisory record `_cell_writers/v1/<cell_id>` + fenced-log attribution | the next incident is readable in one line | low |
| 7 | **§5** regression test — 3 promotable nodes, 1 cell, concurrent writes, exactly one epoch bump | none; guards all of the above | none |

Commit 2 is the addition. Splitting the publisher out of (e) is what makes the
rollout safe, and it has a second benefit: the heartbeat objects can be watched
in the bucket for a day before any code depends on them, which is the cheapest
possible way to find out that the clock, the path or the readiness gate is wrong.

Commits 3 and 4 are the pair that must not be separated by a release. 3 tells
clients where to send writes; 4 makes a node refuse writes it should not take.
Landing 3 without 4 means routing points at the right node while every other
node still steals the epoch from it — no worse than today, but no better either.
Landing 4 without 3 means correct refusals that drivers cannot act on, because
the routing table still names the wrong writer.

### What each stopping point actually leaves in prod

|                                    | What lands                                                | What prod looks like after                                                                 |
| ---------------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| **All of Phase 1**, staged commits | §3, publisher, (a)–(e), 3a, §5 regression test            | ping-pong fixed, drivers re-route correctly, regression guarded                            |
| **Crate only** (§3)                | `heartbeat.rs`, `directory.rs`, tested against `InMemory` | unchanged — ping-pong continues                                                            |
| **Crate + (b)**                    | §3 plus the don't-promote rule                            | ping-pong stops; non-owner writes fail opaquely and drivers retry into the same wrong node |

`crate + (b)` deserves a second look because it is the tempting small option and
it is a trap. It does fix the duel. But the failure it substitutes is a client
that receives a refusal it cannot interpret, holds a routing table that names
the wrong writer, and retries into the same node until it gives up. That is a
different outage with the same symptom — writes not landing — and it is arguably
harder to diagnose than the ping-pong, because the epoch counter stops moving
and the logs go quiet. The observable "epoch climbing forever" signature that
made the original incident findable disappears.

### Where the review effort actually goes

Not evenly across the seven commits. Commits 1, 2, 5, 6 and 7 are mechanical —
new code in a new crate, a background task, a backoff constant, a PUT, a test.
The scrutiny belongs on 3 and 4, which together are perhaps 60 lines that change
who is allowed to write. Splitting them out is what makes the full set reviewable
despite being larger than the alternatives.

### My recommendation

**All of Phase 1, staged as the seven commits above.** The crate on its own
changes nothing in prod; `crate + (b)` trades a visible failure for a quiet one.
The full set is a bigger review, but the commits are individually small, the two
that matter are isolated, and the last one is the incident reproduced as a test.

### Your answer

<!-- write here -->

---

## Carried over — two smaller decisions, not yet agreed

Both surfaced while answering Q1 and Q2. Neither blocks starting on §3, but both
must be settled before commit 4.

### A. A failed node LIST — shed ownership, or keep it?

Sleet keeps its current assignments when the LIST fails (`daemon.rs:243`,
`warn!("failed to LIST nodes/: {e}; keeping current assignments")`). For a
compaction coordinator that is a safe double-run. For a **writer** it is an
unbounded duel: a node partitioned from the object store can never learn it lost
ownership, so it re-promotes forever against a node that believes it won. There
is no convergence, because convergence requires both views to update and one of
them cannot.

Proposal: **shed.** If we cannot read the live set we cannot claim to own
anything — refuse promotion, return `NotCellWriter` with no owner hint. This
deliberately trades availability for the bound, and it is the one place this
design must diverge from its reference implementation.

The counter-argument worth stating: a node partitioned from the object store
cannot write to SlateDB either, so shedding may be moot in the common case. It is
not moot for a *partial* failure — LIST throttled or 503-ing while writes still
succeed — which is the case the rule exists for.

### B. Does `with_preferred_writer_node` survive?

Proposal: **keep it as an explicit override above rendezvous.** Useful for tests
and single-node deploys, and it fails closed by validating the id against the
address map. What must go either way is the `reachable.first()` fallback beneath
it (`routing.rs:236-241`) — that fallback moves whenever a probe flaps and is
half the instability Q2 is deleting.

## After you answer

Q1, Q2 and Q3 are folded into §7 of
`docs/plans/2026-07-25-rendezvous-placement.md` and into section H of
`interactive/write-routing-placement.html`. This file stays as the decision
record — the reasoning lives here, the settled outcome lives in the plan.

Outstanding: **Q4**, plus the two carried-over decisions above. Q4 gates how much
of Phase 1 lands in the next pass; A and B gate commit 4 specifically, not the
start of the work.
