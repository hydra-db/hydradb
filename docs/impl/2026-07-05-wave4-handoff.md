---
title: "turbolay — Wave 4 handoff (short)"
date: 2026-07-05
kind: handoff
status: awaiting user approval — nothing implemented yet
related:
  - 2026-07-05-m1-gap-closure-plan.md
---

# Wave 4 — what it is, in one page

**"Waves" = the batches of parallel-subagent implementation work within M1.**
Waves 1–3 built the M1 write path (commits `440145b` xid-resolution int#1,
`dc256ac` D3 merge operator, `4b1abb0` D4 posting ops, `7bb05d6` D5/D6 atomic
write path + recovery; 127 lib + 35 integration tests green). Wave 3's
recurring nits: unused imports under `-D warnings`, and SlateDB tests that
can hang (use a bounded monitor).

**Wave 4 = closing the last honest M1 gaps.** Three workstreams, fully
specified in `2026-07-05-m1-gap-closure-plan.md` (the binding doc — all open
decisions in it are already resolved against code evidence):

| WS | What | Key resolved decisions |
|---|---|---|
| **A** | `EdgeProp = 0xC` companion record for valued/faceted edges (RFC 0005 acc #6) | single-copy `[src][pred][dst]` key (**amends RFC 0005** — RFC edit included); **no ChangeRecord format change**; `delete_node` orphans documented → RFC 0012 scope |
| **B** | `tests/slatedb_acceptance.rs` — durable gate, zombie fencing, crash recovery (RFC 0004 acc #1/#2/#3), **plus valued-edge `EdgeProp` companion end-to-end + recovery** (RFC 0005 acc #6 on real SlateDB — closes the in-memory-only coverage gap WS A left) | tempdir **required** (in-memory object store is fresh per open — can't work); add typed `StorageError::Fenced` to the vendored `common` fork; commit is already `await_durable: true` |
| **C** | RFC 0017 **Phase 0** observability (currently only a tracing scaffold exists) | `metrics` facade + instrumented ObjectStore wrapper (via new `with_object_store` seam in vendored common) + write fan-out timers + honest invariant-counter subset |

**Execution:** A ∥ B as two parallel Sonnet agents (disjoint files); **C runs
after A** (both touch `write.rs`). Agents end with test/clippy/fmt clean and
do **not** commit; integrator verifies and commits per workstream:
`M1 D7: EdgeProp companion record` · `M1: SlateDB acceptance tier` ·
`M1: RFC 0017 Phase 0 observability spine` ·
`M1: cover valued-edge companion end-to-end on real SlateDB`.

**After Wave 4:** M1 is fully closed → M2 (index framework, RFC 0006).
Still outstanding and *not* in this wave: RFC 0017 Phase 3 real-S3 benchmark
harness (hard gate before any optimization RFC).

**Context docs written this session:** `../goals.md` (north-star pillars),
`../dgraph-alignment.md` (revisit after implementation — esp. §4b facet
read-amplification, which lands with workstream A),
`2026-07-05-graphblas-experiment-sketch.md` (later, feeds RFC 0009).
