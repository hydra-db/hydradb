# HANDOFF — turbolay book

Status as of 2026-07-06. Read `AGENTS.md` (mission + the bar) and `SCRATCHPAD.md`
(working plan) alongside this.

## Where we are

**Part I — The Intuition Track is COMPLETE.** Ten chapters, written, compiling to
`main.pdf` (~37 pp), each rendered and eyeballed. Build: `make build` (needs the
`typst` CLI; `marginalia`/`fletcher` fetch once then cache).

| File | Chapter | Lands |
|------|---------|-------|
| `intro-00-orientation.typ` | The One Sentence | the one-sentence architecture; the two tracks |
| `intro-01-uid-sets.typ` | Everything Is a UID Set | edge = set membership; traversal = set algebra; Neo4j/FalkorDB/us; `<fig-cast>` |
| `intro-02-roaring-dense-uids.typ` | Roaring Bitmaps and Dense UIDs | roaring as a map of chunks; why UIDs must be dense; UidPack traded away; `<fig-roaring>`, `<tab-density>` |
| `intro-03-graph-model.typ` | Nodes, Edges, and the Keyspace | node = one blob; the flat tagged keyspace; reject-vs-split; `<fig-keyspace>`, `<fig-limits>` |
| `intro-04-s3-substrate.typ` | The Substrate: SlateDB on S3 | rent-the-engine; no value log → the caps; the writer fence; `<fig-stack>`, `<fig-division>` |
| `intro-05-write-path.typ` | The Write Path | one atomic fan-out; tombstone delete; merge-vs-RMW; seq token; `<fig-fanout>` |
| `intro-06-single-writer.typ` | What One Writer Deletes | the deletion ledger, honestly costed; `<fig-deletion>` — **the heart** |
| `intro-07-indexes.typ` | Indexes Are Posting Lists Too | value/reverse/count; tokenizers; the watermark; `<fig-indexes>`, `<fig-tokenizers>` |
| `intro-08-read-path.typ` | The Read Path and the Consistency Trick | `(≤W − deleted) ∪ reeval(W,latest]`; worked trace; modes; `<fig-trick>`, `<fig-modes>` — **the climax** |
| `intro-09-reader-writer.typ` | The Writer/Reader Split | stateless readers over shared S3; closes the arc; `<fig-fleet>` |

## The workflow that produced this (repeat it for Part II)

Strictly **sequential, one chapter at a time**:
1. Spawn ONE `general-purpose` subagent as a domain-expert sparring partner. Give it
   the chapter's thesis, the reader's prior context, and a numbered battery of hard
   questions that (a) probe the intuition, (b) force the Dgraph/Neo4j/FalkorDB
   contrast, and (c) demand a **built-vs-design line**. Ask for deliverables:
   sharpened intuitions, a table or two, vivid micro-examples, and **overreach
   warnings**. The subagents repeatedly caught real errors — trust that pass.
2. Write the `.typ` from the distilled material — never a transcript dump.
3. `make build`, render pages to PNG, and **visually review** (the box/table/figure
   layout matters as much as the prose).
4. Update `SCRATCHPAD.md`; move to the next chapter.

## The bar and voice (locked — see `AGENTS.md` "The bar")

Open on a *felt problem*; vivid concrete imagery; make the solution feel inevitable;
motivate even a non-technical reader. **Not preachy, not poetic** — cut
meta-commentary ("sit with it"), keep concrete images (the sorted shelf, the
apartment buildings, the ledger written by one hand). **Scannable, recall-friendly
section headings** ("Queries are set algebra", not "A question becomes arithmetic").
Every structural idea gets a `#figure` + caption + `<fig-slug>`; reuse the cast
colors (Source green, Chunk blue, Entity amber). Each chapter ends on a curiosity
gap into the next.

## Running dataset (reuse everywhere)

Dense UIDs 1–6: `source_1`=1, `chunk_10`=2, `chunk_11`=3, Ada=4, Engine=5,
Babbage=6. Edges: `chunk_10 MENTIONS {4,5}`, `chunk_11 MENTIONS {5,6}`,
`entity_7/Ada RELATES {6}`, `entity_19/Engine RELATES {6}`, `source_1 HAS_CHUNK {2,3}`.

## Correctness notes discovered along the way (do NOT reintroduce these errors)

The grounding subagents corrected several tempting-but-wrong claims. Hold Part II to
them:
- turbolay is **subject-major** (`EdgeOut[src][pred]`), **not** predicate-sharded.
  "predicate-sharded" is Dgraph's language / a marketing one-liner; there is no
  sharding (single writer, one DB per namespace).
- Traversal is **set algebra** (∪ expands, ∩ constrains, − excludes), not "just
  intersection." A plain forward walk is a union.
- turbolay **never pointer-chases** — every hop is a SlateDB/S3 `get`.
- **`EdgeProp` IS built** — single-copy `EdgeProp[src][pred][dst]` (an amended
  shape; RFC 0004 vs original RFC 0005 disagreed — code won).
- Node record = **labels + props + xid only**; monolithic; **1 MiB cap = REJECT**
  vs adjacency **512 KiB = SPLIT** (opposite policies).
- Degree counter is a **simplified `Meta` i64 sum** (code is i64, RFC D11 says i32 —
  trust the code), **not** the RFC 0006 `Count` bucket index, and it's non-idempotent.
- **Value/Count indexes are NOT built** (M2/RFC 0006); `merge.rs` fail-closes on
  them. Only **reverse (`EdgeIn`)** is a live index. Index lag is a **backfill-time**
  phenomenon — live indexes are maintained in-batch and don't lag.
- Read path: only **one-hop `neighbors` (set − deleted)** is built (`posting_ops.rs`).
  The freshness gate, W-math, changelog-tail overlay, planner, IR, and openCypher
  executor are **RFC 0001/0007 design** (M2/M3). `decypher` is vendored but not wired.
- Consistency is **read-your-writes + monotonic reads**, NOT linearizable/serializable.
  Strict mode = "freshest within one poll interval," not linearizable.
- The writer fence is a **manifest-epoch CAS**, detect-on-next-write — **not**
  consensus, not leader election, not HA/instant-failover. Multi-writer (RFC 0016)
  is a future stub, not built.
- SlateDB pin is **0.14.x** (lockfile 0.14.1); the 1 MiB node cap is **turbolay
  policy**, not a SlateDB limit (SlateDB allows 4 GiB values).
- Node codec is a **hand-rolled fail-closed LE codec**, not bincode.

## Typst gotchas hit (avoid re-hitting)

- `#sym.*` inside a `` `code span` `` renders **literally** — use ASCII (`>=`, `<=`,
  `->`) inside backticks; `#sym` only in prose/markup.
- Term lists `/ Term: desc` need a colon — use `- *bold.* text` bullets instead.
- A `#figure` whose body contains `raw()` auto-labels as "Listing"; force it with
  `kind: image, supplement: [Figure]` (or accept `kind: raw, supplement: [Listing]`
  for actual code/batch dumps).
- `#info-box` stamps a "Note" title by default; pass `title: [Real Title]` or
  `title: none`.
- Part names in `gen-index.sh` should NOT start with "Part I ·" — bookly already
  prefixes "Part 1 –".

## What's next — Part II (The Implementation Track)

Not started. Same architecture, now in bytes/code/RFC for the implementer. Filenames
`detail-NN-slug.typ` → auto-grouped under "The Implementation Track". Concept N in
Track II pairs with concept N in Track I. Planned chapters (see `SCRATCHPAD.md` ToC):
II·1 Posting values (`posting.rs`,`value.rs`,RFC 0005) · II·2 UIDs & identity
(`ids.rs`) · II·3 Node & schema records (`schema.rs`) · II·4 Substrate & keyspace
(`storage.rs`,`serde/`) · II·5 The write batch (`merge.rs`) · II·6 Seq, fencing &
the dropped-Dgraph ledger · II·7 Index framework (RFC 0006) · II·8 Planner & read
path (RFC 0007) · II·9 Fleet & roles (RFC 0008).

## Optional polish still open (low priority)

- **Interlinks**: chapters already cross-reference by number and chain via
  cliffhangers/callbacks; a dedicated pass could add explicit "recall …/paid off in
  …" links and Typst `@fig-slug` references now that all targets exist.
- A cover/logo (`title-page` currently `logo: none, cover: none`).
- Vendoring `fletcher`/`marginalia` for a fully offline/CI build (currently fetched
  once and cached).
