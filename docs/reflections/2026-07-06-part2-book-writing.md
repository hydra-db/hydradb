# Reflections — writing Part II of the design book (2026-07-06)

Learnings from starting Part II (The Implementation Track). Captured so the next
session (and the next chapter) doesn't relearn them.

## Process that works
- **Pipeline per chapter:** code-grounded brief → draft (subagent) → Opus proofread
  against source → machine-verify any flagship byte figure → build/render → human
  review → tune → next. Each stage caught something the previous missed.
- **Cadence (locked by the user):** go slow, **one chapter at a time**, via a
  subagent; the user reviews each, we adjust tone, then proceed. Do not batch-write
  Part II. Chapter 2 onward: draft with an **Opus** subagent.
- **Grounding subagents that die mid-read can be resumed** via SendMessage (their
  file-reading context is intact) — far cheaper than a fresh spawn. But a shared
  account **session limit** can kill a whole parallel wave at once; don't fan out 9
  heavy agents when one-at-a-time is the agreed cadence anyway.
- **Machine-verify, don't hand-derive, the flagship figures.** The 34-byte
  `PostingValue` layout in II·1 was hand-traced first; a throwaway
  `println!` test on the real serializer (`PostingValue::single(set_of(&[4,5]))`)
  confirmed it byte-for-byte. Worth it — the book's credibility rests on "we refer
  to the actual code."

## Correctness discipline (the whole point for this audience)
- Audience = **database experts (Neo4j / PlanetScale)**. Overreach loses them
  instantly. **Code wins over RFC** on every conflict; say so in the prose.
- **Mark built-vs-design honestly.** Opus proofread caught II·1 overstating that
  `split`/`rollup` run on the SlateDB acceptance tier — they're only in in-memory
  unit tests (`tests/slatedb_acceptance.rs` has zero split/rollup refs). This is
  exactly the kind of claim that must be true.

## Two source corrections found while grounding (carry into later chapters)
- **The session/read token is a bare `u64`.** `src/serde/token.rs` is the
  *index-token* codec (exact/int/float/hash tokenizers), **not** the session-token
  codec. Do not conflate them in II·6/II·8.
- **The writer-epoch fence has no code in `src/`.** It is a SlateDB manifest
  property described only in RFC 0001; its test is explicitly blocked. II·6 must
  present the fence as **design**, not built — a manifest-epoch CAS,
  detect-on-next-write, not consensus/leader-election/HA.

## Book-production learnings
- The **reader theme had two real bugs**, now fixed globally in
  `book/vendor/bookly/src/themes/reader.typ`: (1) block code rendered white-on-cream
  (invisible) — now a dark terminal card; (2) figure captions had *no* styling and
  read as body prose — now bold accent label + italic muted body. Fix theme once,
  every chapter benefits.
- **Learning-goals signpost** added as a required opening element per chapter (a
  compact `#info-box(title: [Learning goals])`), so a reader can pick chapters by
  interest. Once more chapters exist, add a Part II **reading-paths** guide built
  from these.
- Typst gotcha reconfirmed: `#sym.*` renders literally inside `` `code spans` `` —
  ASCII only inside backticks.

## Status
- **II·1 "Posting values" — DONE** (Ch. 11): written, styled, Opus-proofread, figure
  byte-verified, builds clean.
- Next: **II·2 "UIDs & identity"** (`src/ids.rs`, RFC 0004) — the `SequenceAllocator`,
  crash-safe dense allocation, gap tolerance, the xid↔uid map. Single writer per
  namespace ⇒ no intra-namespace allocation race; the interesting part is
  crash-safety and why dense-with-gaps is fine.
