---
title: "turbolay — North-Star Goals"
status: living
date: 2026-07-05
related:
  - plan.md
  - dgraph-alignment.md
  - rfcs/0000-rfc-index.md
---

# North star: Dgraph on S3, object-native

The one-sentence goal: **reimplement Dgraph's graph-on-KV storage model as an
object-native database — S3 is the storage layer, we are the compute layer.**

Every design decision should be checkable against this document. If a proposal
fights one of the four pillars below, the proposal is wrong or it needs an RFC
that consciously amends this document.

## What "object-native" buys us — the four pillars

### 1. KV store on S3 — SlateDB

An ordered KV store (point `get`, prefix `scan`, atomic `WriteBatch`,
merge operators) whose WAL, SSTs, and manifest **all live under one S3
prefix**. No local durable state anywhere. SlateDB v0.14.1, used strictly
unmodified (D1) — anything it doesn't expose is an application-level protocol
on its public APIs, never a fork.

*Owned by RFC 0002.*

### 2. Value log on S3 — the LSM itself is the log

The durable log-structured store — WAL + LSM levels — is itself on object
storage. SlateDB today, "or otherwise" in principle: RFC 0002's escape hatch
keeps migration open, cheapest toward another ordered-KV/LSM substrate.

**Conscious clarification (so we don't confuse ourselves later):** this is
*not* Badger's WiscKey-style value log. SlateDB has **no key/value
separation**, and we deliberately do not reimplement one (D10 — "value-log
subsumed by SlateDB"). The accepted price: large values are rewritten whole by
compaction → node property size is capped (RFC 0004), posting lists split at
512 KiB (RFC 0005), oversize spill-to-raw-S3 is backlog (RFC 0014). "Value log
on S3" means *the durable log lives on S3*, not *we have a vlog*.

*Owned by RFC 0002 (constraints), 0004 (cap), 0005 (split), 0014 (spill).*

### 3. Reader/writer split — because the durable log is shared

Since all durable state is one S3 prefix, compute splits cleanly:

- **Exactly one writer per namespace** (D2). Writes serialize by
  construction. This is what lets us delete Dgraph's entire distributed
  half — Raft groups, the Zero timestamp oracle, conflict OCC,
  version-in-key MVCC.
- **Stateless readers** open the same prefix via `DbReader` (unfenced,
  manifest-polling) and scale independently of the writer (D9). Kill or
  replace any compute node at will.
- The **only coordination primitive** is SlateDB's manifest `writer_epoch`
  CAS — a zombie writer is fenced with `CloseReason::Fenced`. No ZooKeeper,
  no DynamoDB, no lock service.

Status note: the split is **in v0 scope** (M3 — RFC 0008's fleet, same binary,
`--role {writer,reader}`), not post-v0. "Later" only in the sense that M1–M2
build the write path first. Multi-writer is explicitly out until RFC 0016's
trigger fires.

*Owned by RFC 0008 (fleet), 0016 (multi-writer, backlog).*

### 4. Strong consistency model

Single writer + durable-log-on-S3 makes strong semantics cheap:

- every durable write returns a **session token** — our own monotonic logical
  seq (`m/latest_seq`), not SlateDB's internal seqnum;
- a reader must not answer a query carrying token `T` before its replayed
  durable state reaches `T` (freshness gate on `durable_seq`);
- **index lag is invisible**: reads = indexes/posting-lists up to a watermark
  + changelog tail `(W, latest]`, merged;
- no token → bounded staleness; strict mode → bounded by
  `manifest_poll_interval` or reader reopen.

*Owned by RFC 0001 (amended by 0004's seq protocol).*

## The derivation chain

The pillars are not independent — each enables the next, and the chain **is**
the architecture:

> LSM/WAL on S3 (2) ⇒ durable state is shared, compute is stateless ⇒
> reader/writer split with one writer (3) ⇒ a single monotonic seq is
> authoritative ⇒ session-token strong consistency without any coordinator (4).

Break any link (e.g. add a second writer, or local durable state on a node)
and the links after it stop holding for free.

## Guardrails when deciding anything

- Does it need a **second writer** per namespace? Out of scope (→ RFC 0016).
- Does it need **local durable state** on a compute node? Breaks
  object-native — compute must stay stateless and killable.
- Does it need a **SlateDB change**? Build it as an application-level protocol
  on public APIs or don't build it (D1). (The M1 vendored fork is
  `opendata-common` — our wrapper layer — *not* SlateDB; SlateDB stays
  unmodified.)
- Is it an **optimization**? It waits for numbers from **real S3**, never
  LocalStack-only (D12, RFC 0017).
