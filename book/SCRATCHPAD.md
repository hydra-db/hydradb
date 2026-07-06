# SCRATCHPAD — turbolay book (working plan, for us)

Living notes while we build the book. Decisions here are ours-in-progress; the
durable mission is in `AGENTS.md`. Update freely.

## What the book is (one line)

turbolay = **Dgraph on S3**: Dgraph's storage model reimplemented in Rust on
SlateDB, with its distributed half (Zero/Raft/MVCC) deleted because there is
exactly one writer per namespace. The book teaches this from first principles,
intuition-first.

## Locked decisions

- **Persona**: a dev who already has `fundamentals-of-graph` background (property
  graphs, LSM, replication, Dgraph internals from its ch12–18). Teach turbolay
  *specifically*: how it's built, why, the tradeoffs. Don't re-teach fundamentals.
- **Method**: *subtraction from Dgraph*. For each subsystem — name Dgraph's
  version → show how it lands on SlateDB/S3 → say what we keep / replace / delete
  and the constraint that bought.
- **RFC treatment**: narrative-first; cite RFC number + `src/*.rs` file where a
  claim needs authority. RFCs are not chapters.
- **Structure**: **two tracks**.
  - **Track I — Intuition** (the persona read; stop here if not implementing).
  - **Track II — Implementation** (the coder read; byte layouts, code, RFCs).
  - Concept N in Track I pairs with concept N in Track II.
- **Comparative lens** (recurring): *what is a traversal, physically?*
  - Neo4j → pointer chase (index-free adjacency).
  - FalkorDB → matrix multiply (GraphBLAS sparse boolean matrix).
  - Dgraph / turbolay → sorted-set intersection (posting lists).
  Draw on Neo4j + FalkorDB to sharpen why we chose the set-algebra answer.
- **Running dataset** (below): one RAG-KG traced through every chapter.

## The running dataset (dense UIDs 1–6)

```
UID 1  source_1   "paper-alpha"        (:Source)
UID 2  chunk_10                          (:Chunk)   HAS_CHUNK  from source_1
UID 3  chunk_11                          (:Chunk)   HAS_CHUNK  from source_1
UID 4  entity_7   "Ada Lovelace"         (:Entity)
UID 5  entity_19  "Analytical Engine"    (:Entity)
UID 6  entity_88  "Charles Babbage"      (:Entity)

Edges:
  chunk_10 MENTIONS {4,5}     chunk_11 MENTIONS {5,6}
  entity_7 RELATES  {6}       entity_19 RELATES {6}
  source_1 HAS_CHUNK {2,3}
```

Canonical query used to show set algebra:
"entities mentioned by chunk_10 that relate to Babbage(6)"
= `MENTIONS_out[2] ∩ RELATES_in[6]` = `{4,5} ∩ {4,5}` = `{4,5}` (Ada + Engine).

## Table of contents

### Part I — The Intuition Track

| # | Chapter | Dgraph anchor | Learning goal |
|---|---------|---------------|---------------|
| I·0 | The one sentence | Dgraph minus its distributed half, on S3 | State turbolay in a line; why log+compaction+single-writer = a whole DB. (no code) |
| I·1 | Everything is a UID set | `posting` (list model) | Edge = set membership; traversal = ∩/∪/−. Keystone. Neo4j/FalkorDB/us contrast. |
| I·2 | Why roaring, why dense UIDs | `codec` (UidPack) + `algo/uidlist` | Replace UidPack w/ roaring; the UUID mistake; set algebra without decompressing. |
| I·3 | The graph model | predicate-as-tablet; triple→posting | Property graph → predicate-sharded KV; node = one value + 1 MiB cap; out/in. |
| I·4 | S3 as the substrate | Dgraph-on-Badger → us-on-SlateDB | What SlateDB owns; no KV sep → no value log → the caps; split = new keys. |
| I·5 | The write path | `worker` mutation apply | One upsert → one atomic WriteBatch fan-out; dense UID alloc; the changelog. |
| I·6 | What single-writer deletes | Zero + Raft + MVCC | Name each deleted piece + the constraint that made deletion safe. Heart of the book. |
| I·7 | Indexes are posting lists too | tokenizers + index predicates | value/reverse/count as the same set machinery; the watermark idea. |
| I·8 | Read path & the consistency trick | query processor + txn reads | Watermark + changelog-tail merge; failure modes (reader-behind, zombie, RYW). |
| I·9 | Writer/reader split (future) | Alpha groups | Stateless compute; readers as caches that replay the log. |

### Part II — The Implementation Track

| # | Chapter | Code / RFC | Contains |
|---|---------|-----------|----------|
| II·1 | Posting values | `posting.rs`,`value.rs` · 0005 | RoaringTreemap, PostingValue codec, format tag, 512 KiB split, set algebra. |
| II·2 | UIDs & identity | `ids.rs` · 0004 | SequenceAllocator, dense u64, crash-safe alloc, xid→uid + recovery. |
| II·3 | Node & schema records | `schema.rs` · 0004 | NodeRecord, monolithic vs wide-column, name interning u32, EdgeOut/In/Part. |
| II·4 | Substrate & keyspace | `storage.rs`,`serde/` · 0002/0003 | SlateDB via common::Storage; GRAPH=0x05; record tags; order-preserving codec. |
| II·5 | The write batch | `merge.rs` · 0004 | WriteBatch fan-out steps, changelog schema, merge-op dispatch, RMW split. |
| II·6 | Seq, fencing & what's deleted | 0001/0004 | m/latest_seq, writer-epoch fencing, D4 seq protocol, formal dropped-Dgraph ledger. |
| II·7 | Index framework | 0006 | IndexAm trait, tokenizers, backfill state machine, per-index watermark commit. |
| II·8 | Planner & read path | 0007/0001 | Cypher read subset → IR, anchor select, per-hop intersect, freshness gate, staleness, tail-merge. |
| II·9 | Fleet & roles | 0008 | DbReader, --role {writer,reader}, error taxonomy, namespace lifecycle. |

Notes: I·0 has no Track-II pair (pure intuition). Filenames:
`intro-NN-slug.typ` → Track I, `detail-NN-slug.typ` → Track II. `gen-index.sh`
gets a two-part split; `intro-` → "The Intuition Track", `detail-` →
"The Implementation Track".

## Workflow: subagent-driven intuition (one at a time)

Per chapter, sequentially:
1. **Spawn one domain-expert subagent** with deep access to the RFCs, the code,
   and the fundamentals repo. Its job: answer a battery of *probing/difficult*
   questions to build our intuition, correct wrong framings, and surface Neo4j /
   FalkorDB / Dgraph contrasts.
2. **Probe further** via follow-ups (SendMessage) until the intuition is airtight.
3. **We (main) write** the crisp, intuition-filled `.typ` chapter from the
   distilled material — not a transcript dump.
4. `make index && make build` to check it compiles; then next chapter.

Order: I·1 → I·2 → … (intuition track first), then Track II details.

## Status / TODO

- [x] Scaffold (`book/` builds, typst 0.15).
- [x] `AGENTS.md` mission written.
- [x] ToC + learning goals + running dataset (this file).
- [x] gen-index.sh two-part (intro-/detail-) split.
- [x] I·0 `intro-00-orientation.typ` (the one sentence).
- [x] I·1 `intro-01-uid-sets.typ` (keystone; `<fig-cast>` diagram).
- [x] I·2 `intro-02-roaring-dense-uids.typ` (roaring + dense UIDs; `<fig-roaring>`, `<tab-density>`).
- [x] I·3 `intro-03-graph-model.typ` (node blob, keyspace, two limits; `<fig-keyspace>`, `<fig-limits>`). Also fixed I·1 facet line (EdgeProp IS built — single-copy `EdgeProp[src][pred][dst]`).
- [x] I·4 `intro-04-s3-substrate.typ` (SlateDB, no value log, writer fence; `<fig-stack>`, `<fig-division>`).
- [x] I·5 `intro-05-write-path.typ` (atomic fan-out, tombstone delete, merge-vs-RMW, seq token; `<fig-fanout>` listing).
- [x] I·6 `intro-06-single-writer.typ` (the deletion ledger, honestly costed; `<fig-deletion>`).
- [x] I·7 `intro-07-indexes.typ` (value/reverse/count as posting lists, tokenizers, watermark; `<fig-indexes>`, `<fig-tokenizers>`). Framed lag as backfill-time (live indexes maintained in-batch); reverse is the one live index.
- [x] I·8 `intro-08-read-path.typ` (the consistency equation, worked trick trace, modes table; `<fig-trick>`, `<fig-modes>`). Fixed `#sym` inside code-span bug (use ASCII ops in backticks).
- [x] I·9 `intro-09-reader-writer.typ` (stateless readers over shared S3, closes the arc; `<fig-fleet>`).
- [x] Polish: info-box "Note" labels fixed (titles / `title: none`); part names de-duplicated in gen-index.
- [x] `HANDOFF.md` written.
- [x] **PART I COMPLETE** — 10 chapters, builds to main.pdf (~37pp), all rendered/reviewed.
- [~] Part II (The Implementation Track) — IN PROGRESS; ToC above; see HANDOFF.md.
  - [x] II·1 `detail-01-posting-values.typ` (Ch.11) — PostingValue codec, 34-byte
    layout (machine-verified), format-tag escape hatch, 512 KiB split, merge-operand
    "code wins" story, honest built-vs-design. Opus-proofread. Pipeline: brief →
    draft → Opus proofread → figure byte-verify → styled → reviewed.
  - Theme fixes landed (reader.typ): block-code dark card + figure caption styling.
    New per-chapter element: "Learning goals" signpost box.
  - [ ] II·2 `detail-02-uids-identity.typ` — next (Opus draft).
- [ ] polish pass: info-box "Note" labels; forward/backward interlinks.

## Voice notes (locked from user feedback)
- Not preachy, not poetic. Cut meta-commentary ("sit with it", "the debt this
  leaves us"). Keep concrete imagery (shelf of ID cards, apartment buildings).
- Section headings must be scannable / recall-friendly from the ToC — plain, not
  clever. (e.g. "Queries are set algebra", not "A question becomes arithmetic".)
- Every structural idea gets a figure (fletcher/CeTZ or native), `#figure` +
  caption + `<fig-slug>`. Reuse the cast colors.
- Each chapter: open on a felt problem, close with a curiosity gap into the next.

## Open questions to revisit

- Exact 512 KiB vs 1 MiB cap numbers as they land in code (confirm against `src/`).
- How much openCypher to show in Track I vs defer entirely to Track II.
- Whether I·2 (roaring/dense-uid) is intuition or belongs partly in Track II.
