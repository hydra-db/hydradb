# AGENTS.md — turbolay design book

This directory is a **Typst / bookly** book. Its job is to turn turbolay's RFCs,
implementation, and background reading into **one first-principles, intuition-first
narrative** about how turbolay works and *why it is built the way it is*.

If you are an agent asked to work in this folder, read this file first. It is the
mission and the method; the build mechanics are at the bottom.

---

## The goal (one paragraph)

Amalgamate three sources — the `fundamentals-of-graph` textbook, the turbolay
**RFCs (0000–0017)**, and the **current codebase (M0/M1)** — into a book that
builds a reader's *intuition* from first principles: data structures and the
choices behind them → the graph model on top → S3 as the substrate → keyspace &
encoding → write path → indexes → read path → (future) writer/reader separation.
The narrative is the product; the RFCs are cited source, not the spine.

## Who the book is for (the persona — this sets the depth of everything)

A developer who **already has the `fundamentals-of-graph` background**: property
graphs, LSM/compaction, replication, posting lists as a concept. Assume that
literacy. **Do not re-teach it.**

What this reader needs from *this* book is everything **about turbolay
specifically**:

- How each piece is actually implemented, and **why**.
- The **tradeoffs we chose** and what we gave up.
- Where turbolay departs from Dgraph and *what that departure buys us*.

Every chapter should leave the reader with a load-bearing mental model, not a
spec dump. Prefer "here is the choice, here is the alternative, here is why we
picked this" over exhaustive enumeration.

## RFC treatment

**Narrative-first.** The prose builds intuition; **reference the relevant RFC by
number wherever a claim needs its authority** (e.g. "the 512 KiB split — RFC
0005"). Do *not* paste RFCs in as chapters. The raw RFCs stay in `docs/rfcs/` as
the source of record; the book points at them. Likewise cite the codebase by
`file:line` (e.g. `src/posting.rs`) so a reader can jump from intuition to code.

---

## The recurring method: *subtraction from Dgraph*

turbolay is **Dgraph's storage model with its distributed half deleted**, because
there is exactly **one writer per namespace**. The single most effective teaching
move in this book is: *name the Dgraph component, say why we deleted or replaced
it, and name the constraint that decision bought us.*

The canonical worked example (use it early, it flips intuition):

> **"How is the value log implemented?"** → It isn't. turbolay has *no* value
> log. Badger needs one because it separates keys from large values; SlateDB does
> **not** do KV separation, so we delete the value log. The price of deleting it
> is two constraints we then design around: a node is stored as one **monolithic
> value with a ~1 MiB size cap** (reject oversize — RFC 0004), and a supernode's
> adjacency is bounded by the **512 KiB posting-list split** (RFC 0005) so
> compaction never rewrites one giant value.

Kept vs replaced vs deleted (the map to teach against):

- **Kept from Dgraph**: `x/keys` order-preserving layout, the posting-list model,
  predicate sharding, delta-then-rollup structure, the triple→KV out/in/index/
  count projection.
- **Replaced**: UidPack `codec` + `algo/uidlist` intersection → **roaring**
  (`RoaringTreemap`); set algebra comes from the library (RFC 0005).
- **Deleted** (single-writer makes them unnecessary): Raft, the Zero timestamp
  oracle, `start_ts`/`commit_ts`, conflict-key OCC, MVCC-in-key, the value log.

## The load-bearing intuitions (make sure the book lands these)

1. **The one sentence.** turbolay is a *predicate-sharded KV store on SlateDB
   where each value is a compressed sorted set of UIDs (a posting list), and a
   graph edge is membership in one of those sets.*
2. **The consistency trick.** An index may lag the log; the lag is never
   *visible*, because a read answers `indexed-state-up-to-watermark W` **plus**
   `changelog tail after W`, merged. Single writer + one logical `seq` replaces
   the entire Dgraph oracle/MVCC apparatus.
3. **Why dense `u64` UIDs.** Roaring compression and cheap set math need
   dense-ish integers; UUIDs would destroy them (a documented prior mistake).
   Users address by external `xid` via an `xid → uid` index.
4. **S3 changes what's cheap.** A "split" is just new keys, never an in-place
   rewrite; compute is stateless and rebuilds by replaying the log.
5. **Correct-first.** Optimizations (CSR, WCOJ/leapfrog joins, bitpacked frames)
   are deliberately deferred behind a `format` tag until measured on **real S3**,
   not intuition.

---

## Narrative spine (working chapter arc — `guide-*` prefix)

Front-to-back read; refine as we write. Filenames use the `guide-NN-slug.typ`
convention so the auto-index groups them under "Reader's Guide".

1. `guide-01` **Mental model** — the one sentence; log + compaction + single
   writer. No code, no keys.
2. `guide-02` **The data-structure choice** — posting lists & roaring sets; edge
   = set membership; dense `u64` UIDs and the UUID mistake.
3. `guide-03` **The graph model on top** — property graph → predicate-sharded
   KV; nodes (monolithic value + cap), typed edges, out/in projections.
4. `guide-04` **S3 as substrate** — SlateDB owns LSM/compaction/fencing/S3; what
   that lets us delete (the value log, the oracle, Raft); no KV separation → the
   caps.
5. `guide-05` **Keyspace & encoding** — order-preserving keys, record-type tags,
   `GRAPH = 0x05`.
6. `guide-06` **Write path** — the single `WriteBatch` fan-out; UID allocation;
   the changelog; `m/latest_seq`.
7. `guide-07` **Index framework** — value/reverse/count as posting lists; the
   per-index watermark commit.
8. `guide-08` **Read path** — openCypher read subset; freshness gate; watermark +
   changelog-tail merge; set-algebra intersect.
9. `guide-09` **Writer/reader separation (future)** — the fleet, `DbReader`,
   role routing.

## The bar (non-negotiable quality standard)

**Part I must make the reader *want* to implement turbolay** — not merely
understand it. Every chapter clears four gates, in this order:

1. **Curiosity, opened then closed.** Lead with a *felt problem* — a tension the
   reader can almost feel — before any solution appears. Close it, then leave a
   sharper curiosity aimed at the next chapter. Satisfy *and enhance* the
   reader's curiosity.
2. **Vivid mental imagery.** The reader should *see* it: the log as a ledger, a
   posting list as a sorted shelf of IDs, a traversal as two shelves zippered
   together. Concrete pictures over abstractions. Reuse the running RAG-KG cast
   (Ada / Babbage / the Engine, UIDs 1–6) so images accumulate.
3. **Intuition so strong the solution feels inevitable.** By the time the
   mechanism arrives, it should read as "of course — what else would it be?" The
   Neo4j / FalkorDB / Dgraph contrast exists to make our choice feel *earned*.
4. **Motivating even to a non-technical reader.** The *why* carries the chapter;
   the *how* rewards the engineer without losing the layperson. Someone
   non-technical should finish Part I thinking "that's elegant."

**Cumulative & sequential.** Chapters are written in order; each inherits the
prior chapter's vocabulary and images so intuition compounds. Interlink forward
("paid off in I·4") and backward ("recall the shelf from I·1") — done as a pass
once target chapters exist. Clarity, vivid problem imagery, strong intuition,
and an earned solution are the measure of every chapter.

## Diagrams (add them wherever they build intuition)

Vivid mental imagery is a gate (see *The bar*), and a diagram is often the fastest
way to hit it. **Whenever a passage describes a structure the reader must
*picture* — a graph, a key layout, a write fan-out, a read-path merge, a state
machine — add a figure.** Do not leave "the tangle", a set operation, or a
multi-step flow as prose-only if a picture would land it faster.

- **Tool: `fletcher`** (node-and-arrow diagrams, built on CeTZ) for graphs and
  flows; drop to raw **CeTZ** for freeform drawing. Import per chapter file:
  `#import "@preview/fletcher:0.5.8" as fletcher: diagram, node, edge`.
- **Version pin matters.** This repo builds on **typst 0.15**, which rejects the
  CeTZ that `fletcher:0.5.5` pins — use **`0.5.8`+**. Both `fletcher` and
  `marginalia` fetch once over the network then cache; a fully offline/CI build
  would need them vendored (backlog).
- **Wrap every diagram in `#figure(..., caption: […]) <fig-slug>`** so it lands in
  the List of Figures and can be cross-referenced (`@fig-slug`) from later
  chapters.
- **Reuse the running cast and its colors** so images accumulate: Source = green
  `#cfe9d5`/`#2f8a4f`, Chunk = blue `#d3e3f7`/`#3b6fb0`, Entity = amber
  `#f7e5c2`/`#c8791f`; edges gray `#8a8f98` (HAS_CHUNK), blue `#3b6fb0`
  (MENTIONS), orange `#c8791f` (RELATES). Nodes show the **UID** big with the
  **xid** beneath. The worked pattern lives in
  `chapters/intro-01-uid-sets.typ` (`<fig-cast>`) — copy it.
- Place the figure so the reader *sees* the thing before or just as the prose
  names it (e.g. the cast graph sits before "This is a tangle…").

## Voice & Typst conventions

- Match the reference book's register: direct, concrete, one idea per box. Study
  `/Users/abhishek/hydradb/2026-06/fts-on-s3/book/chapters/guide-01-mental-model.typ`
  before writing.
- Every chapter file starts with
  `#import "../vendor/bookly/src/bookly.typ": *` and a single top-level `= Title`.
- Use the bookly idioms: `#info-box[…]`, `#tip-box[…]`, `#boxeq[…]` for the
  load-bearing equation, `#note[…]` for margin sidenotes. Reserve `#boxeq` for the
  one line a chapter wants the reader to remember.
- Cite sources inline: RFC number for decisions, `src/…rs` for implementation.

---

## Sources (absolute paths)

- **RFCs (spec of record)**: `/Users/abhishek/hydradb/graphdb-on-s3/turbolay/docs/rfcs/` — start at `0000-rfc-index.md` (the map).
- **Implementation notes / plan**: `/Users/abhishek/hydradb/graphdb-on-s3/turbolay/docs/impl/`, `docs/plan.md`, `docs/open-decisions.md`.
- **Codebase (M0/M1)**: `/Users/abhishek/hydradb/graphdb-on-s3/turbolay/src/` (`posting.rs`, `value.rs`, `ids.rs`, `schema.rs`, `storage.rs`, `merge.rs`, `serde/`) + `tests/`.
- **Textbook background** (assume the reader has this; mine for framings/contrast): `/Users/abhishek/hydradb/graphdb-on-s3/fundamentals-of-graph/chapters/`.
- **Format reference** (mechanism + voice to imitate): `/Users/abhishek/hydradb/2026-06/fts-on-s3/book/`.

## Workflow

1. **Chat first, write second.** Build the intuition for a chapter in
   conversation before committing it to `.typ`. The narrative is the hard part;
   the Typst is mechanical.
2. Write one chapter file per topic under `chapters/`, named `guide-NN-slug.typ`.
3. **Never hand-edit `chapters/_index.typ`** — it is auto-generated. Run
   `make index` (or `./scripts/gen-index.sh`) after adding/removing a chapter.
4. Build to check: `make build` → `main.pdf` (needs the `typst` CLI). `make watch`
   for live rebuild while writing. `*.pdf` is gitignored — commit `.typ` sources.
