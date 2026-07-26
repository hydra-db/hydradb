---
title: Rendezvous Phase 1 follow-ups — review
date: 2026-07-26
branch: Turbolay-V3.5
base_commit: a143313
head_commit: 4ab2c2b
plan: docs/plans/2026-07-25-rendezvous-placement.md
status: complete
tags:
  - routing
  - placement
  - single-writer
  - fencing
  - review
---

# Rendezvous Phase 1 follow-ups — review

Covers `d3ce0ca..4ab2c2b`, the five commits that came out of re-reading Phase 1
against its own two planning documents. The findings themselves are recorded in
`2026-07-26-rendezvous-phase-1-review.md`; this is a review of the fixes.

| Commit | What |
|---|---|
| `d3ce0ca` | justfile exports the FFI build environment; `test-server-runtime` recipe |
| `f0a1843` | `NodeReadiness` — decision 7's withdrawal, wired |
| `b638fd8` | the re-open ladder armed where re-opens actually fail; stale fence doc deleted |
| `08c1dfe` | plan and review corrected where they described code that was never wired or later reverted |
| `4ab2c2b` | routing refusals classified transient rather than as a syntax error |

## The one that changes production behaviour

`f0a1843`. Everything else is either build tooling, dead code, documentation, or
an error code.

`/readyz` now answers 503 when placement has shed, which is decision 7's
"go unready" arriving at the endpoint that word refers to. **The consequence
worth stating out loud is correlated:** the shed condition is a failing LIST
against the shared object store, so a store-wide LIST outage takes *every* node
unready within one grace window, and k8s drains every Service endpoint at once.
That is the specified behaviour and it is bounded — the view recovers on the
first successful LIST, and `/livez` is untouched so nothing restarts — but it is
a new way for the fleet to go dark together, and it did not exist before this
commit. Bolt is unaffected either way: drivers hold pod addresses and never
traverse the Service.

The alternative would be to withdraw the heartbeat without failing `/readyz`,
which keeps HTTP serving from a node that refuses every write. That is worse and
also incoherent — placement's whole argument is that readiness and liveness ride
one channel — but it is the trade someone should be aware was made.

## Judgement calls a second reader should check

**The ladder now delays writes it did not delay before.** A failed writer open
arms 2s, and the next write to that shard waits it out before promoting; past
one `heartbeat_interval` the wait is refused with `AdmissionRejected` instead.
Before `b638fd8` a failing open was retried once per write with no pacing. This
is the behaviour decision 6 rule 1 describes, and the damping is the point, but
it converts a fast failure into a slow one for the first few attempts.

**Two routing refusals are transient and one is not.** No addressable live node
and no live owner are `RoutingUnavailable`; an owner with no configured Bolt
address stays `UnsupportedQuery`. The split rests on a claim worth checking: that
the third case is unreachable in `graph-node`, because it builds the directory
from `bolt_node_addresses.keys()`, so the two operands cannot disagree. If an
embedder can construct a fleet where the address map legitimately lags the
directory during a rollout, that case is transient too and the classification is
wrong.

**`Neo.TransientError.General.DatabaseUnavailable` is a choice, not a
derivation.** The class is what matters — drivers branch on `TransientError` vs
`ClientError` — and the code within the class is the closest honest description.
Any transient code would produce the same driver behaviour.

**The `#[allow(clippy::useless_conversion)]` at `opencypher.rs:361` is
platform-conditional by nature.** It is correct on macOS, where bindgen types the
constant as `u32`, and inert on Linux, where the conversion is real. Linux CI
proves the second half; nothing local can.

## Verified

Through `just`, from a stripped environment: 136 lib, 305 under
`public-client-protocols`, 67 placement, 11 bin, 225 opencypher. `cargo clippy
--all-targets --features server-runtime,indexer-runtime -- -D warnings` clean,
which it was not before `f0a1843`. `cargo fmt --all --check` clean.

New tests, four: the readiness transitions, a shed view withdrawing a healthy
node's heartbeat, the transient class of a routing refusal (asserting the Bolt
code, not just the variant), and the config half of that same split. The
existing shed-routing test changed with the behaviour rather than being deleted.

## Not verified, and how it would be

**No real driver has seen the new routing code.**
`scripts/bolt_neo4j_driver_smoke.sh` does drive a `neo4j://` URI through the
official Python driver, which is exactly the right harness — but it runs against
`examples/bolt_compat_server.rs`, which serves a static `BoltRoutingServer` and
never touches `ObjectStoreBoltRoutingTableProvider`. So "the driver moves to the
next router" is reasoned from the error class, not observed. Pointing that smoke
test at a provider that can be made to refuse would close the gap, and it is the
cheapest real check available.

**The re-open ladder's new arming site has no test.** Proving it needs a store
whose `Db::open` fails, and that is the third hand-rolled failing store this work
would need — `engine::placement` and `graph_node/readiness.rs` each already have
one because `FaultStore` is `#[cfg(test)]` inside `crates/placement`. Graduating
it to `crates/test-support`, which decision 11 anticipates and which this makes
overdue, would make the test cheap.

## Left alone

- `GRAPH_HEARTBEAT_INTERVAL_MS` / `_TIMEOUT_MS` still appear in no chart or
  manifest. Defaults apply, so behaviour is correct and the knobs are
  unreachable.
- `interactive/write-routing-placement.html` still reads "crate landed, Q1–Q4
  decided, wiring next" and marks question B open. The plan names it as a
  Source, so it is stale in a place that matters.
- No demotion path, and decision 8's startup window can still name a
  gracefully-removed peer as an owner for one `heartbeat_timeout`. Both recorded
  in the Phase 1 review.
- `install_writer` now has exactly one caller. It was split out for the retry
  loop that no longer exists; folding it back into `promote_writer` is a tidy-up
  nobody needs today.
