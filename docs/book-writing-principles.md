# Book writing principles — Part II

Chaithanya's directives for Part II (The Implementation Track). Keep to these.

## Audience
- **Database experts, not normies.** Write for a senior engineer from **Neo4j** or
  **PlanetScale/Vitess**. Assume LSM, MVCC, Raft, query planning, posting lists.
  Don't re-teach fundamentals — teach turbolay *specifically*.

## Voice & craft
- **Clarity is the superpower.** Think twice, then explain plainly. If a thing is
  subtle, slow down and make it obvious.
- **Build intuition first**, then show the bytes/code. Make the mechanism feel
  inevitable before it arrives.
- **Include detail when it builds intuition** — not for completeness. Explain nicely.
- **Diagram wherever it illustrates.** A picture beats a paragraph for byte layouts,
  key encodings, fan-outs, state machines, the tail-merge. Reuse the running cast.
- Tone matters. Not preachy, not poetic. Concrete images, scannable headings.

## Grounding
- **Refer to the actual code we wrote in turbolay.** Cite `src/file.rs:line`.
  When an RFC and the code disagree, **the code wins** — say so.
- Mark **built vs designed** honestly (M1 built vs RFC/M2/M3 design). The expert
  trusts the book only if it never claims something is real that isn't.

## Cadence (how we work)
- **Go slow. One chapter at a time, written via a subagent.**
- After each chapter: Chaithanya reviews → we adjust tone → then proceed to the next.
- Method throughout: *subtraction from Dgraph* — name Dgraph's version, show how it
  lands on SlateDB/S3, say what we keep/replace/delete and the constraint it bought.
