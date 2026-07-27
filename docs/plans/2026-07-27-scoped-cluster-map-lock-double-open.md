---
title: The scoped-cluster map lock and the double-open race
status: draft-for-review
date: 2026-07-27
branch: Turbolay-V3.5
base_commit: 8d7e939
tags:
  - concurrency
  - scoped-clusters
  - writer-fencing
  - metrics
---

# The scoped-cluster map lock and the double-open race

## Sources

Everything below rests on reading these at `8d7e939`. Line numbers in this file
are from that tree and `src/engine/cluster.rs` has drifted before; prefer the
symbol names.

| File | What it holds |
|---|---|
| `src/engine/cluster.rs` | `ScopedRoutedGraphCluster::cluster_for_scope` (`:1182`), the `clusters` mutex and its LRU eviction (`:1188`–`:1238`), `local_shard_runtime_metrics` (`:1265`), `loaded_clusters` (`:1256`), `close` (`:1289`), and `RoutedGraphCluster::open_at_path` (`:301`) — the shard-open loop the lock is held across |
| `src/shard/lifecycle.rs` | `GraphShard::open_internal` (`:121`) and its `GraphWriteAuthority` match (`:146`–`:154`) — the fact that a *promotable* open opens nothing; `acquire_local_write_guard` (`:254`), `promote_to_writer` (`:476`), `validate_write_fence` (`:541`) |
| `src/core/state.rs` | `GraphStore::lazy` (`:250`), `promote_writer` (`:555`) → `install_writer` (`:581`), `refresh_writer_fence` (`:395`) and its `Fenced` arm (`:426`–`:443`), `open_reader` (`:613`), `close` (`:754`), `GraphWriteAuthority` (`:947`) |
| `src/core/config.rs` | `open_graph_db` (`:350`) — `Db::builder(..).build()`, the one call that claims a SlateDB writer epoch |
| `docs/plans/2026-07-25-rendezvous-placement.md` | Decision 3 (`### 3. Lease — 3a, advisory record only`, ~`:410`): there is **no fence-before-open hook** on `Db::builder`, `build()` claims a new epoch unconditionally, and the advisory record under `_cell_writers/v1/` is diagnostic only |
| `docs/plans/2026-07-26-otel-metrics-span-links-and-alerting.md` | Where BUG-2 was filed (`:205`–`:218`) and the interval-collection design that will take this mutex every export interval (`:790`–`:815`) |
| Commit `08e78df` | BUG-1, the sibling lock-scope fix, and the `futures::poll!` test standard this plan matches |
| `src/bin/graph_node/admin.rs:203`, `src/bin/graph-node.rs:425`, `src/bin/graph_node/config.rs:201` | The three non-query takers of the mutex: `/metrics`, the index-discovery loop, and `GRAPH_MAX_OPEN_SCOPES` (default **8**) |
| Memory: `cell-writer-fencing-pingpong` | The production incident whose signature a self-fence would reproduce from a single node |

## Verdict

Three findings, and they do not point the same way as the bug report.

1. **The hold BUG-2 names is not expensive.** `open_promotable_scoped_with_memory_options`
   performs **no object-store I/O at all** and never yields. The multi-millisecond
   shard open the report assumes does not happen on the promotable path.
2. **A double open *would* be a correctness problem, not merely wasteful.** Two
   live promotable clusters for one scope can each claim the SlateDB writer
   epoch for the same cell, and the second claim fences the first. The process
   fences itself, and the symptom is indistinguishable from the
   `cell-writer-fencing-pingpong` incident except that every trace carries the
   same `turbolay.node_id`.
3. **There *is* a multi-millisecond hold in `cluster_for_scope`, and it is a
   different line.** The LRU eviction closes a whole cluster — every shard's
   SlateDB reader and writer — under the same mutex. And the metrics collector
   has a second, sharper interaction with that eviction that can turn a scrape
   into a client-visible `AdmissionRejected`.

So: do not build options 1–4. They are correct fixes to a hold that costs
microseconds, and each of them puts the invariant that prevents (2) at risk in
exchange. Fix (3) instead, and pin (1) with a test so the day someone adds I/O
to the promotable open, the test says so rather than production.

## 1. What is actually under the lock

`cluster_for_scope` holds `clusters` across three different things. They are not
remotely the same cost.

**The hit path** — `clusters.get_mut(scope)`, bump `last_used`, clone the `Arc`.
A `BTreeMap` lookup. Every query takes this, and it is nanoseconds.

**The open** — `RoutedGraphCluster::open_promotable_scoped_with_memory_options`,
`:1216`–`:1230`. Follow it down:

- `open_at_path` (`:301`) validates the node id and cell ids against
  `ObjectStoreNodeDirectory`, which is two in-memory `BTreeSet`s
  (`src/engine.rs:80`); `contains_node` (`cluster.rs:174`) is a set lookup.
- `self.placement.clone()` is an `Arc` clone — `PlacementView` is
  `{ shared: Arc<Shared> }` (`src/engine/placement.rs:194`).
- For each cell it calls `GraphShard::open_promotable_with_memory_options`
  → `open_internal` with `GraphWriteAuthority::Promotable`. The authority match
  at `lifecycle.rs:146` is:

  ```rust
  GraphWriteAuthority::ReadOnly    => { let _ = db.open_reader().await?; }
  GraphWriteAuthority::Promotable  => {}
  GraphWriteAuthority::Writer      => { db.promote_writer().await?; }
  ```

  The promotable arm is empty. The store is `GraphStore::lazy` (`state.rs:250`),
  which stores the path, the object store handle and the config with
  `writer: None` and `reader: None`. Nothing is opened.
- The drop-marker read at `cluster.rs:363` is guarded by `!promotable &&`, so
  short-circuiting skips it entirely on this path.
- The rest of `open_internal` is struct construction: a handful of `Semaphore`s
  and `Mutex<BoundedGraphCache>`es, and `BoundedGraphCache::new_with_byte_limit`
  (`src/core/cache.rs:217`) builds two empty `BTreeMap`s regardless of the
  capacity it is given.

**A promotable open therefore issues zero requests and contains no await that
can pend.** Its cost is `O(cells in the node directory)` allocations of empty
maps — tens of microseconds at a realistic cell count, and the future is `Ready`
on its first poll. That is a testable property, and §6 tests it.

**The eviction** — `:1194`–`:1214`, taken only when `clusters.len() >=
max_open_scopes`. It picks the least-recently-used entry whose
`Arc::strong_count == 1`, `try_unwrap`s it, and calls `cluster.close().await`
**with the mutex still held**. `RoutedGraphCluster::close` (`:1077`) closes every
shard; `GraphShard::close` → `GraphStore::close` (`state.rs:754`) closes the
`DbReader` and then the `Db`. That is a WAL flush and a manifest write per shard,
against object storage, serialised, under a mutex that every query takes.

This is the multi-millisecond hold. It is in the same function, twenty lines
above the one the bug report names.

It is also a cliff rather than a slope. Below `max_open_scopes` (default 8) it
never runs. Above it, every miss pays a full close-then-open cycle under the
lock, and every query on every other scope waits behind it. A node serving nine
warm scopes behaves categorically worse than one serving eight.

Note that `ScopedRoutedGraphCluster::close` (`:1289`) already gets this right:
`std::mem::take(&mut *self.clusters.lock().await)` drops the guard at the end of
that statement and closes the drained entries with the mutex free. The pattern
this plan wants already exists in the same file.

## 2. Is a double open a correctness problem? Yes.

The chain is short and every link is in this tree.

Writer promotion does not happen during the open. It happens later, in
`RoutedGraphCluster::ensure_local_writer` (`cluster.rs:458`), called from
`cluster_for_scope_write` (`:1241`) **after** `cluster_for_scope` has returned
and released the mutex. It reaches `shard.promote_to_writer`
(`lifecycle.rs:476`) → `GraphStore::promote_writer` (`state.rs:555`) →
`install_writer` (`state.rs:581`) → `open_graph_db` (`config.rs:350`), which is:

```rust
Db::builder(path, object_store).with_settings(settings).build().await
```

Decision 3 of the rendezvous plan states the consequence explicitly: *"there is
no fence-before-open hook on `Db::builder`. `build()` claims a new epoch
unconditionally."* The epoch lives in the manifest and carries no identity. Two
`build()` calls on the same path produce two epochs, and the older handle is
fenced — regardless of which process, or thread, made them.

Two clusters for the same scope hold two independent `GraphStore`s for the same
path. `GraphStore::lazy` builds a fresh `Arc<GraphStoreInner>` with its own
`writer` slot and its own `writer_open_gate`; there is no process-wide registry
keyed by path. The gates that make promotion safe — `writer_open_gate`,
`WriterReopenGate`, the `local_write_guard` mutex, the `writer_lanes` — are all
**per `GraphShard` instance**. None of them can see a second instance.

So: if both clusters are used for writes, the second `ensure_local_writer` fences
the first. The first's next `acquire_local_write_guard` (`lifecycle.rs:254`)
calls `refresh_writer_fence`, which takes the `Fenced` arm (`state.rs:426`),
drops the handle, arms the re-open backoff and returns a `fencing`-class error.
The next write re-promotes and fences the other one back. That is the ping-pong,
inside one process, with a shorter period than the network version because there
is no rendezvous check to damp it — `resolve_placement` (`cluster.rs:527`)
answers `Local` for both, because both are this node.

The advisory record makes it diagnosable and misleading at once:
`log_fence_attribution` (`state.rs:493`) would name `last_promoted_by` as **this
same node**, which reads as a corrupt record rather than as what it is.

**Answer, definitively: a double open is a correctness problem the moment either
copy is written through.** A double open of two clusters where only one is ever
written through is merely wasteful — some tens of microseconds and a few empty
`BTreeMap`s. The distinction is *reachability of the second copy*, not the open
itself, and that is exactly what the current lock guarantees.

### The map, and what a second insert would do

`clusters` is `BTreeMap<GraphScope, ScopedRoutedClusterEntry>` (`engine.rs:114`).
A second insert for the same scope **replaces** the entry; the displaced
`ScopedRoutedClusterEntry` is dropped, decrementing its `Arc`. So:

- The first cluster is *not* dropped if any caller still holds a clone — and
  callers do, for the length of a query. It becomes an unreferenced-by-the-map,
  still-live, still-promotable cluster. That is precisely the reachable second
  copy above.
- It is never `close()`d. Nothing calls `close` except eviction and
  `ScopedRoutedGraphCluster::close`, both of which work from map entries.
- It breaks two invariants that today are load-bearing. Eviction's
  `Arc::strong_count(&entry.cluster) == 1` test stops meaning "idle", because a
  displaced clone is not counted. And `ScopedRoutedGraphCluster::close`'s
  `Arc::try_unwrap` starts failing with `"scoped cluster is still in use"` on
  shutdown.

Incidentally, the `CorruptValue` arm on `Arc::try_unwrap` in the eviction path
(`:1208`) is unreachable today, and for an instructive reason: between the
`strong_count == 1` filter and the `try_unwrap` there is no await, and no `Arc`
clone can be minted without the mutex. **The map lock is the interlock.** That is
the property any narrowing has to reproduce.

### What dropping a half-opened shard costs

Nothing, and I can be specific. There is no `impl Drop` for `GraphShard`,
`GraphStore` or `RoutedGraphCluster` anywhere in `src`. A cluster that was opened
promotable and never written through holds `writer: None` and `reader: None`, so:

- **No lease is released, because none was taken.** The promotable open claims no
  epoch. There is nothing in the object store that names it.
- **No lease dangles.** Dropping it leaves no `_cell_writers/v1/` record (that is
  written only after a successful promotion, `cluster.rs:597`) and no manifest
  entry.
- `GraphStore::close` on such a store is itself a no-op — both branches at
  `state.rs:754`–`:762` are `None`.

Dropping a cluster that *has* promoted is a different matter: it leaks an open
`Db` handle with its background tasks, and it does not un-claim the epoch —
there is no un-claim, only supersession by a later `build()`. That case must not
arise, which is why any design here must close what it discards.

## 3. The interaction nobody has filed yet

`local_shard_runtime_metrics` (`:1265`) takes the mutex only to clone the `Arc`s
out, then releases it — that part is already right. But it then **holds those
`Arc`s for the whole collection**, and the eviction candidate filter is
`Arc::strong_count(&entry.cluster) == 1`.

So while a scrape is collecting, every open scope is un-evictable. If the map is
at `max_open_scopes` and a query arrives for a scope that is not open, eviction
finds no candidate and `cluster_for_scope` returns

```
AdmissionRejected { operation: "open_graph_scopes", actual: 9, limit: 8 }
```

straight to the client. Not a stall — a hard error, with no retry and no wait.
The same applies to `loaded_clusters` (`:1256`), which the index-discovery loop
(`src/bin/graph-node.rs:425`) calls on an interval and holds across
`dirty_graph_index_edge_types` and `discover_graph_index` per cell, which *do*
perform I/O. That window is far wider than the metrics one.

This is the collector moving a user-visible failure rate rather than a latency
percentile, and it survives every one of the five proposed options, because none
of them touch it. It is worth more than BUG-2.

## 4. The options

Evaluated against the finding that the open costs microseconds and no I/O.

**1. Double-checked locking.** Drop the guard, open, re-acquire, and if another
task won, return *theirs* and drop yours. Cheap to implement and correct — but
only if the loser returns the winner's `Arc`. Return your own while inserting
theirs, or insert yours over theirs, and §2 says you have built the self-fence.
The correctness of the whole scheme rests on a discarded value never escaping,
which is an invariant a reviewer has to hold in their head rather than one the
types enforce. It also has to re-run the capacity check after re-acquiring, and
it lets `clusters.len()` transiently exceed `max_open_scopes` — the divisor for
the per-scope cache budget (`options_for_scope`, `:1176`). Cost to buy: microseconds.

**2. Per-scope `tokio::sync::OnceCell`.** Store `Arc<OnceCell<Arc<RoutedGraphCluster>>>`
in the map; take the lock only to get-or-insert the cell, then
`get_or_try_init` outside it. Exactly one open ever runs, waiters coalesce, and
a failed open leaves the cell empty so the next caller retries. This is the
structurally correct answer: "one open per scope" becomes a property of the type
rather than of a code path. The costs are real though — the LRU's `strong_count`
test has to reach through the cell, `Arc::into_inner` + `OnceCell::into_inner` at
eviction, an empty cell after a failed open has to be removed or it occupies a
slot forever, and `local_shard_runtime_metrics` / `loaded_clusters` have to skip
uninitialised cells. That is a real amount of new state to buy microseconds.

**3. `Shared<BoxFuture>` per scope.** Does not compile without contortions.
`futures::future::Shared` requires `Output: Clone`, and the output here is
`Result<RoutedGraphCluster, GraphError>`; `GraphError` derives only
`Debug, Error` (`src/core/error.rs:8`). It would need `Result<Arc<..>, Arc<GraphError>>`
and an error-shape change at the boundary. It also keeps a completed future's
output alive in the map, which is a second place a cluster is reachable from —
the exact hazard of §2. Reject.

**4. In-flight set plus `Notify`.** A hand-rolled option 2 with more states and
the same semantics, minus the type-level guarantee, plus the classic
lost-wakeup surface if the notify is armed after the waiter checks. Strictly
worse than 2. Reject.

**5. Leave it; make the collector not take this lock.** The narrow one. But the
collector's critical section is *already* narrow — clone the `Arc`s and get out.
Making it narrower buys nothing. What the collector actually needs is the
opposite of a lock change: it needs to stop *retaining* what it took (§3).

None of 1–4 addresses the eviction close, which is where the milliseconds are.

## 5. Recommendation

**Reject the framing, fix the two things that are real.**

- Do not implement 1–4. The double-open guard they buy defends against a race
  whose only motivation is narrowing a critical section that costs microseconds.
  If the promotable open ever acquires I/O, option 2 is the design to reach for,
  and step 1 below makes that day loud.
- Do implement the eviction narrowing (step 2), which is where the millisecond
  hold is — but only with the closing-slot state, because eviction is the one
  place where a released lock genuinely can produce two live handles for one path.
- Do fix the collector's `Arc` retention (step 3), independently and first if
  only one thing gets done. It is the only item here that moves an error rate.

## 6. Plan

### Step 1 — pin "the promotable open does no I/O"

The property the whole recommendation rests on is nowhere written down and is one
token from being broken: deleting `!promotable &&` at `cluster.rs:363`, or filling
in the empty `Promotable` arm at `lifecycle.rs:150`, silently reintroduces the
bug as reported.

Add a test in `mod scoped_cluster_tests` (`cluster.rs:1325`) that pins it in the
`08e78df` style — drive the future by hand, no second task, no timer:

```rust
let mut open = std::pin::pin!(RoutedGraphCluster::open_promotable_scoped_with_memory_options(..));
assert!(futures::poll!(open.as_mut()).is_ready(),
    "a promotable open must complete without yielding: it opens no SlateDB \
     reader or writer, and the `clusters` mutex is held across it");
```

A single `poll!` returning `Ready` proves no await inside pended, which for this
code path is exactly "no I/O". Pair it with a negative control — the same assert
against `open_readers_scoped`, which *must* be `Pending` on its first poll
because `open_reader` is on its path — so a test that starts passing vacuously is
caught. Neither test involves the scheduler, so neither can flake.

**Done when:** both asserts pass, and reverting `!promotable &&` at `:363`
locally makes the promotable one fail.

### Step 2 — move the eviction close out of the critical section

Change the map value from a cluster to a slot:

```rust
enum ScopedSlot {
    Ready { cluster: Arc<RoutedGraphCluster>, last_used: u64 },
    Closing(Arc<tokio::sync::Notify>),
}
```

Eviction: pick the victim as today, replace its slot with `Closing(notify)`,
**drop the guard**, `close().await`, re-acquire, remove the slot, `notify_waiters`.
A caller that finds `Closing` clones the notify, drops the guard, awaits it, and
retries the whole function.

The tombstone is the point. Removing the entry before the close completes would
let a concurrent `cluster_for_scope` for that same scope open a fresh promotable
cluster and promote it while the old cluster's `Db` is still open — §2's
self-fence, arriving through the back door, and with an added hazard that the old
handle's in-flight close would then be flushing against a store whose epoch has
moved. `await_durable_writes` is `true` by default (`config.rs:320`) so no
acknowledged write is at risk, but the close would fail and the failure is
currently swallowed.

Two secondary consequences to accept explicitly:

- `cluster_for_scope` becomes a retry loop rather than straight-line code. Bound
  it (one wait, then `AdmissionRejected`) so a wedged close cannot hang the read
  path indefinitely.
- The `Arc::try_unwrap` `CorruptValue` arm stops being unreachable: with the
  guard dropped, a `Ready` slot's count can no longer be assumed stable across
  the call. Keep the check; it becomes a real branch.

**Done when:** a test holds the mutex-free window open deterministically —
victim slot swapped to `Closing`, a second `cluster_for_scope` for a *different*
scope polled once with `futures::poll!` and observed `Ready`, proving it did not
queue behind the close; and a second `cluster_for_scope` for the *closing* scope
polled once and observed `Pending`, proving the tombstone holds it off. Plus:
`ScopedRoutedGraphCluster::close()` still drains cleanly afterwards.

### Step 3 — stop the collector pinning every scope

`local_shard_runtime_metrics` and `loaded_clusters` should collect
`Weak<RoutedGraphCluster>` under the lock and upgrade one at a time, dropping
each `Arc` before moving to the next. That shrinks the un-evictable window from
"every open scope, for the whole collection" to "one scope, for one cluster's
worth of cache-lock reads" — and an upgrade that returns `None` means the scope
was evicted mid-collection, which is a skip, not an error.

This does not eliminate the window; eliminating it needs an explicit in-use count
separate from the `Arc` count, which is more machinery than the residual risk
justifies at `max_open_scopes = 8` and a 60s export interval. Say so in the
comment rather than implying the race is gone.

The index-discovery loop (`graph-node.rs:425`) matters more than the metrics
path here, because it holds its clones across real I/O. Same change.

**Done when:** a test at `max_open_scopes = N` with N scopes open holds a
collection future parked mid-flight (one cache mutex held, `poll!` once, as in
`08e78df`) and shows `cluster_for_scope` for scope N+1 still succeeding rather
than returning `AdmissionRejected`. Against the current code it returns the
error, which is the regression the test exists to pin.

### Step 4 — amend the OTel plan — **done, 2026-07-27**

The BUG-2 entry in `docs/plans/2026-07-26-otel-metrics-span-links-and-alerting.md`
now records that the promotable open is I/O-free, that the eviction close is
where the milliseconds are, and that the double-open concern is a self-fence
rather than waste. §3's finding is filed there as **BUG-4** with its own entry,
is repeated in that document's §5.5, and is named a **prerequisite for M2**
rather than a neighbour of it. §1.5's "the interval task will occasionally block
for a shard open" is retracted in place. Steps 1–3 remain unimplemented.

The original text of this step, for the record: `:205`–`:218` says a
shard open is multi-millisecond and that the interval task "will occasionally
block for a shard open". Both are wrong for the promotable path. Amend BUG-2 in
place — that file already carries corrections in this style — to say the open is
I/O-free, the eviction close is not, and the collector's real exposure is
`AdmissionRejected` rather than latency. Otherwise M2's "measure the collector
against read-path p99" step goes looking for a stall that is not there.

**Done when:** the BUG-2 entry names the eviction close and step 3's admission
interaction, and no longer claims the open costs milliseconds.

## 7. What would change this

Any of these turns option 2 from over-engineering into the right answer, and
should be treated as re-opening this document:

- The promotable open acquiring I/O — a drop-marker check, an eager reader open,
  a manifest probe, a scope-directory read moved earlier. Step 1's test is the
  tripwire.
- `max_open_scopes` being raised well past the number of hot scopes, or the
  directory growing to hundreds of cells, which turns the per-cell allocation
  loop into something worth measuring.
- A second `ScopedRoutedGraphCluster` in one process over the same base path. The
  single-writer-per-path property is enforced by *this one map*, and nothing
  outside it knows that.
