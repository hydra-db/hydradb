---
name: chapter-framing
description: >
  The macro skeleton of a teaching chapter, extracted verbatim from the
  superseded turbolay-v1 book (../turbolay-v1/chapter-01.typ, chapter-03.typ)
  whose pedagogy we want back. Defines the declarative-claim title, the
  operational opening scenario, the "Problem N" ladder where each solution
  creates the next problem, the synthesis section, the honest-boundary section,
  and the revision notes — plus the prose rhythm that carries them. Use BEFORE
  writing or restructuring a chapter, and when a chapter reads like a summary
  instead of a lesson. Triggers: "the chapter lost the teaching", "restore the
  v1 structure", "outline this chapter", "expand this into a real chapter",
  "why does this read like a summary".
---

# Chapter framing (the argument shape, before the prose)

`formatting-chapters` owns the Typst wiring. `writing-content` owns the voice
and the grounding discipline. This skill owns the layer between them: **the
shape of the argument** — what sections exist, in what order, and why each one
earns the next. Read this before you outline; read `writing-content` before you
write the sentences.

## Why this exists

`chapters/intro-*.typ` are ~105–173 lines each. They compressed ~500-line v1
chapters into summaries and lost the teaching. Compare
`../turbolay-v1/chapter-01.typ` (496 lines) with
`chapters/intro-00-replaceable-compute.typ` (108 lines): the intro chapter
*asserts* the durable/compute boundary in a `#boxeq` at line 12; the v1 chapter
*derives* it across six problems, each of which the reader feels before the
mechanism arrives. Length is not the goal — the derivation is. A chapter that
states conclusions is a reference page; a chapter that earns them is a lesson.

**Read-only exemplars:** `../turbolay-v1/chapter-01.typ` and
`../turbolay-v1/chapter-03.typ`. Never edit `../turbolay-v1/` — it is a
historical reference.

**The decision in force:** the conceptual book is being restored to full v1
depth (~500 lines per chapter, the `Problem N` ladder). The compression to ~107
lines is being reversed, not preserved. Do not re-compress.

**Caveat on every v1 quotation in this skill.** The sentences quoted below are
exemplars of *prose shape only*. They describe a pre-resync system and several
are now factually false: leases, deltas, the outbox, the cell write lock and the
two-sequence epoch model were all deleted by the graph-kernel resync. Copy the
rhythm, never the claim. `AUTHORING-NOTES.md` is the authority on what is true
at HEAD.

---

## The skeleton

```typ
= <Declarative claim>                      // not a topic label

<opening scenario in concrete operational terms>
<the question it forces>
<the "we solve those problems in order" hinge>

== Problem 1: <concrete failure as a full sentence>
== Problem 2: …
…                                          // 6–8 total, each created by the last one's fix
== The complete <X> model                  // synthesis
== What this <X> guarantees—and what it does not
== Revision notes
=== The ideas to remember
=== <X> in <N> lines / The compact equations   // optional, where a compact form helps
=== Common confusions                          // optional
=== A quick correctness test
```

### 1. Title: a declarative claim

Not "Architecture", not "The write path". A sentence the chapter argues for:

- `= The Graph Must Survive the Compute Node` (`../turbolay-v1/chapter-01.typ:4`)
- `= One Cell, One Write Boundary` (`../turbolay-v1/chapter-03.typ:4`)

The title is the thesis. By the last section the reader should be able to say
why it is true and where it stops being true.

### 2. Opening scenario — a machine, not a concept

Open with something operational the reader can picture, in the present tense,
with the loss stated as consequences rather than as an abstraction:

> "Imagine a graph node serving a busy application. It has parsed queries in
> memory, recently read graph structures on its local disk, and several active
> requests halfway through execution. Then the machine disappears.
>
> The process is gone. Its memory is gone. Its local disk may be gone. A new
> process starts somewhere else with no useful local state."
> — `../turbolay-v1/chapter-01.typ:6-11`

Chapter 3 does the same with a request instead of a machine — a concrete edge
insertion, `1 -[FOLLOWS]-> 2`, then the observation that the request *sounds*
indivisible while the storage work is not (`chapter-03.typ:6-21`).

Then **the question the chapter answers**, on its own line:

> "What does it mean for the replacement to serve *the same graph*?"
> — `chapter-01.typ:13`

> "…once the correct writer has been identified, how does one logical change
> become one durable fact?" — `chapter-03.typ:23-25`

Then **the hinge** — an explicit promise that the chapter is a sequence:

> "The architecture becomes easier to understand when we solve those problems in
> order." — `chapter-01.typ:19-20`

The hinge is doing real work: it licenses the `Problem N` headings and tells the
reader that each section is a step, not an independent topic.

### 3. The Problem ladder — 6 to 8 sections

Heading form: `== Problem N: <the failure, stated as a sentence>`. Never a noun
phrase. Real examples:

- `Problem 1: a fast machine is not a durable graph`
- `Problem 2: remote truth alone is too slow to query`
- `Problem 3: one giant graph creates one giant coordination problem`
- `Problem 4: a write needs one visible version`
- `Problem 5: complete history is correct but too expensive`
- `Problem 6: multiple nodes can all reach the same object store`
  (`chapter-01.typ:22,73,127,185,247,305`)
- `Problem 2: reaching the transaction is not permission to commit`
- `Problem 3: "one writer" still needs conflict detection`
- `Problem 7: concurrency should queue before it conflicts`
- `Problem 8: a batch changes throughput, not the boundary`
  (`chapter-03.typ:84,147,379,427`)

**The ordering rule: each problem is created by the previous problem's
solution.** Chapter 1's ladder is the model — put truth in the object store (P1)
→ now it is too slow (P2) → so cache, but caching plus one global write boundary
does not scale (P3) → so partition into cells, but a reader inside a cell still
sees torn state (P4) → so epoch every write, but replaying all history is
absurd (P5) → so build artifacts, but shared storage means two nodes can reach
one cell (P6). If you can reorder your sections freely, you have written a topic
list, not a ladder.

**The internal shape of each problem section, in three beats:**

1. **State the failure concretely.** Usually "Suppose…" or "Now consider…" with
   a numbered sequence the reader can follow:
   > "1. a reader begins while the graph contains edge `1 -> 2`; 2. a writer
   > deletes that edge; 3. the reader asks for the edge's reverse index; …
   > If each read uses the newest state independently, one logical query can
   > combine values from before and after the deletion. The result is not a
   > snapshot of any real graph state." — `chapter-01.typ:190-197`
2. **Show why the obvious fix is insufficient.** Name the naive answer, then the
   question it raises:
   > "The obvious response is caching. But caching introduces a dangerous
   > question: what happens when a cache is wrong or empty?" — `chapter-01.typ:80-81`
   > "It is tempting to think that a lease and a lock make transaction isolation
   > unnecessary. They make conflicts less common, but correctness cannot depend
   > on their timing being perfect." — `chapter-03.typ:149-151`
3. **Then introduce the mechanism, which now earns its keep.** Define its jargon
   in a `#term(...)` at that moment, quote the code that implements it, and
   close with the invariant in a `#boxeq[...]`.

Do not lead a problem section with its mechanism. The mechanism is the payoff.

### 4. The component vocabulary inside a problem

| Need | Component | Example |
|---|---|---|
| One-line invariant that falls out of the problem | `#boxeq[ *…* ]` | `chapter-01.typ:36-39`, `209-212`, `272-275` |
| First use of jargon | `#custom-box(title: [Term — Name], icon: "info")[ … ]` | `chapter-01.typ:41`, `145`, `201`, `264` |
| Rationale behind a design choice | `#custom-box(title: [Why], icon: "tip")[ … ]` | `chapter-01.typ:121-125`, `chapter-03.typ:139-145` |
| Real quoted source | fenced block introduced by an inline `file:lines` sentence | `chapter-01.typ:104`, `chapter-03.typ:111`, `219`, `328` |

(The v1 examples cited above use the retired `#term` / `#why` / `#srcblock`
helpers. The *shape* is what you are copying; write it in the `custom-box`
vocabulary — see the helper note at the end of this section.)
| Mapping / comparison / fan-out | `#figure(table(...), caption: […])` | `chapter-01.typ:56-67`, `chapter-03.typ:61-75` |

Notes on each:

- **`#boxeq`** carries the *rule*, not a summary: "Durable state answers what the
  graph is. Compute state makes answering faster." (`chapter-01.typ:36-39`); "A
  read at epoch N sees the durable state that was committed at or before N, not
  whichever individual key happens to be newest when it is read."
  (`chapter-01.typ:209-212`). One per problem section at most; a chapter that
  boxes everything boxes nothing.
- **The Term box** is a definition from first principles at the moment of first use —
  never a glossary dumped up front. v1 defines *Durable state* and *Compute
  state* only after Problem 1 has made the distinction necessary
  (`chapter-01.typ:41-52`).
- **The Why box** answers "why this way and not the obvious way", and is where the
  design's conflicts get named: "The guards are not six versions of the same
  lock. Authority rejects the wrong actor. … Removing one layer changes a
  different property." (`chapter-03.typ:139-145`).
- **Quoted source** — verify the line numbers by opening the file, and name the
  location in the introducing sentence. Mark abridgements there too:
  "…`src/shard/write.rs:2257-2294` (abridged)" (`chapter-03.typ:111` shows the v1
  form of the same idea).
- **Tables** in v1 almost always have a "what breaks" column — *Failure if
  published alone* (`chapter-03.typ:66`), *What it prevents* (`chapter-03.typ:95`),
  *What it must never assume* (`chapter-01.typ:384`). That column is the teaching.

> Helper vocabulary (settled): **all three books converge on Bookly's
> `custom-box`.** `term` / `why` / `srcblock` come from `books/template.typ` and
> still appear in some `inside` chapters (`chapters/01-architecture.typ:1`), but
> they are retired and a separate pass is converting those calls over. Write
> `#custom-box(title: [Term — X], icon: "info")` and
> `#custom-box(title: [Why], icon: "tip")`, and quoted source as a fenced block
> introduced by an inline `file:lines` sentence — see `formatting-chapters`.
> **Pass no `color:` argument**: the accent is derived from `icon:`
> (`vendor/bookly/src/themes/reader.typ:149-165, 421`), so `color:` is silently
> ignored. **The structure in this skill is unchanged by any of this; only the
> component names change.**

### 5. `== The complete <X> model` — synthesis

Assemble the answers into one thing the reader can carry: a layer table with a
"what it must never assume" column plus a numbered request lifecycle
(`chapter-01.typ:375-413`), or a numbered ten-step walk of the real method
followed by a table grouping every step under three questions
(`chapter-03.typ:464-495`). Close with the chapter's central `#boxeq` — the
thesis from the title, now earned:

> "*A cell is a write boundary because authority, graph meaning, epoch, delta,
> and retry result cross the durable boundary in one transaction.*"
> — `chapter-03.typ:499-502`

Follow it with the sentence that says composition, not any single mechanism, is
the source of the property: "The lock alone does not create that property.
Neither does the lease, the epoch, or the transaction in isolation."
(`chapter-03.typ:504-506`).

### 6. `== What this <X> guarantees—and what it does not`

Two bulleted lists, in this order and with this framing:

- "The design guarantees a useful, bounded contract:" — six items, each a
  capability (`chapter-01.typ:417-424`).
- "It does not claim more than the code provides:" / "It does not promise:" —
  six items, each a limit someone might wrongly assume (`chapter-01.typ:426-433`,
  `chapter-03.typ:522-531`).

Then one sentence insisting the limits are structural, not caveats: "Those
limits are not footnotes. They are part of the architecture's shape."
(`chapter-01.typ:435`). Use the em-dash-no-spaces form in the heading exactly as
written: `guarantees—and what it does not`.

### 7. `== Revision notes`

Opens with one line naming its use: "Use these notes to reconstruct the
chapter's argument quickly." (`chapter-01.typ:441`) / "…from the outside in."
(`chapter-03.typ:538`).

- `=== The ideas to remember` — 5–7 bullets, each a **bolded claim followed by
  its two-sentence explanation**: "*Truth and acceleration are different.*
  Durable graph meaning belongs in SlateDB on the object store. Memory, local
  disk, hydrated structures, parsed plans, and caches may disappear and be
  rebuilt." (`chapter-01.typ:445-448`). Not fragments — the bold sentence must
  stand alone as the takeaway.
- `=== The compact equations` / `=== <X> in seven lines` — a two-column
  Question → Revision-answer table condensing the chapter to memorizable form
  (`chapter-01.typ:465-479`), or a Stage → question table walking the mechanism
  (`chapter-03.typ:564-579`).
- `=== Common confusions` — where a mechanism is routinely mistaken for a
  neighbouring one, contrast them explicitly. v1 does this inline (the lease vs.
  lock vs. fence table, `chapter-03.typ:196-207`); promote it to its own
  subsection when a chapter has two or more such pairs.
- `=== A quick correctness test` — a numbered list of 6–7 questions to ask of
  *new* work, not of the chapter: "When evaluating a new feature, ask: 1. If the
  compute node vanishes, can durable state reconstruct the answer? …"
  (`chapter-01.typ:481-490`); "When adding a new mutation path, ask: …"
  (`chapter-03.typ:581-591`). This is what makes the chapter operational.

**End the chapter on a `#boxeq`**, one sentence, the thesis in its most compact
form:

> "*Durable truth makes recovery possible; cell-local epochs and transactions
> make reads coherent; disposable acceleration makes the recovered system
> practical.*" — `chapter-01.typ:492-496`

---

## The prose rhythm

Structure without this rhythm still reads like a spec. Six habits, each with a
v1 quotation.

**1. Present tense, operational register.** Things happen now, to a machine.
> "The process is gone. Its memory is gone. Its local disk may be gone."
> — `chapter-01.typ:10-11`

**2. Short declarative sentences; the punch is a short one after a long one.**
> "These records are different access paths to one logical fact. They must not
> be allowed to acquire separate meanings." — `chapter-03.typ:44-45`
> "That unit is a *cell*." — `chapter-01.typ:143`

**3. Second person for the reader's own reasoning, first-person plural for the
chapter's moves.** "we solve those problems in order" (`chapter-01.typ:19`), "We
need a unit that is both" (`chapter-01.typ:138`), "We can now assemble the
architecture without relying on a slogan" (`chapter-01.typ:377`). Reserve *you*
for the reader's position in the scenario and for the correctness tests.

**4. "Suppose…" / "Now consider…" to open a hypothetical.**
> "Suppose every edge lookup is a fresh object-store operation."
> — `chapter-01.typ:75`
> "Now consider writes. A single edge insertion is not one physical record."
> — `chapter-01.typ:129-130`
> "Suppose a cell has one million historical edge changes." — `chapter-01.typ:252`

**5. State the naive approach before the real one, and say why it fails.** This
is the habit that most distinguishes v1 from the current intro chapters.
> "If both kinds live only in the process, the graph disappears with the
> process. If both kinds live only on a remote object store, every query
> repeatedly pays the cost of finding and decoding remote data." — `chapter-01.typ:30-32`
> "If every graph write shares one global lock, unrelated parts of the graph
> block one another. If every writer updates records independently, readers can
> observe a half-written edge." — `chapter-01.typ:134-137`
> "Blindly repeating `CREATE` or a counter update can apply the logical operation
> twice. Refusing all retries makes transient failures unnecessarily permanent."
> — `chapter-03.typ:264-266`

The pattern is a two-sided squeeze: name both bad extremes in parallel
sentences, and let the mechanism be the thing that escapes them.

**6. Name the limit right after the claim, in the same section.** Do not defer
every caveat to the boundary section.
> "Cell isolation has a precise limit: it does not create a transaction across
> cells." — `chapter-01.typ:181-182`
> "The lane is a concurrency optimization, not the durable correctness
> boundary." — `chapter-03.typ:400-401`
> "The artifact is not a second definition of the graph. It is a shortcut for
> reconstructing a defined version of the graph." — `chapter-01.typ:289-290`

The "X is not Y, it is Z" construction is the workhorse for this and appears
throughout: "`GraphShard` is therefore not merely a cache and not merely a
database handle." (`chapter-01.typ:117-118`); "The outbox … is not incidental
audit logging. It is the durable bridge…" (`chapter-03.typ:365-367`).

**Anti-patterns.** No hype, no "simply"/"obviously", no marketing. No
bullet-dumping a concept that deserves a derivation. No forward references that
excuse a missing explanation ("as we will see"). No mechanism introduced before
its problem.

---

## Pre-flight checklist

- [ ] Title is a declarative claim, and the chapter argues for it.
- [ ] Opening is an operational scenario, ends in a question, then an explicit
      "in order" hinge.
- [ ] 6–8 `== Problem N: <sentence>` sections.
- [ ] Sections cannot be reordered — each one's fix creates the next one's
      problem. Write the chain out and check it.
- [ ] Every problem section runs failure → why the obvious fix falls short →
      mechanism.
- [ ] Jargon defined in a Term box at first use, never before it is needed.
- [ ] Each problem yields at most one `#boxeq` invariant.
- [ ] Every mechanism claim is grounded in a quoted source block with verified
      line numbers.
- [ ] A Why box wherever a reader would ask "why not the obvious way?".
- [ ] Synthesis section assembles the answers and restates the title's claim as
      an earned `#boxeq`.
- [ ] Boundary section has both lists, and a line saying the limits are
      structural.
- [ ] Revision notes have bolded-claim takeaways and a correctness test aimed at
      *new* work.
- [ ] Chapter ends on a one-sentence `#boxeq`.
- [ ] Diagram/figure conventions follow `skills/diagrams/SKILL.md`.
- [ ] Voice and grounding follow `skills/writing-content/SKILL.md`.
