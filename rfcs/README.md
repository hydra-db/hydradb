# RFCs

An RFC is a short design document, written before the code, for the small set
of changes whose consequences outlive the release that ships them.

HydraDB's durable state lives in someone else's object store. Manifest layout,
WAL format, index generations, writer-lease semantics: those decisions survive
every release and every one of us. This directory is where the reasoning behind
them is written down, so the next person reads it instead of re-deriving it
from the code.

Most contributions need nothing here. Read "When an RFC is required" and, if
you are still unsure, open an issue and ask. That answer is free and fast.

## When an RFC is required

Three categories:

1. **The on-disk or wire format changes.** Manifests, the WAL, CSC index
   generations, lease records, the Bolt surface, the HTTP query API.
2. **Query semantics change, or anything user-visible is removed.** A `MATCH`
   that returns different rows than it did last release is a format change by
   another name.
3. **The work takes more than a week, or spans multiple components and the way
   they interact.** Both halves of that test are things you can assess yourself
   before writing any code.

## When an RFC is not required

Bug fixes. Documentation. Refactoring that does not change behaviour.
Performance work: bring a benchmark number rather than a design document; the
[benchmark site](https://hydra-db.github.io/benchmark/) is the bar. New Cypher
functions that fit the existing evaluation model. Anything you could describe
completely in the pull request body.

Being on this list is not a judgement about how valuable the work is. It is a
statement that a written design would not have changed what you built.

## Process

1. **Open an issue.** Always, including for maintainers. The issue is where the
   problem gets argued and where you find out whether an RFC is wanted at all.
   If the answer is no, you have spent one issue instead of one weekend.
2. **Write the RFC.** Copy [`TEMPLATE.md`](TEMPLATE.md) to
   `rfcs/YYYY-MM-DD-kebab-case-title.md`, dated the day you started it. Open a
   pull request. Reference the issue with `see #NNN`, never `closes #NNN`: the
   issue outlives the RFC and tracks the implementation.
3. **Review.** Two maintainer approvals and a seven-day comment window, so that
   people in other time zones get a real chance to object. Discussion happens
   in inline comments on the pull request, which is why the template asks you
   to keep each heading to one point.
4. **Merge as proposed.** The file lands loose in `rfcs/`. This means the design
   was reviewed and is worth building. It does not mean it exists.
5. **Build it.** Incrementally, in small pull requests, against the tracking
   issue. Until the RFC is accepted, keep the code behind a feature flag or a
   name marked unstable, so nobody's data depends on a design that may still
   move.
6. **Move it to `accepted/`.** In the pull request that finishes the work, once
   there is a tested implementation on `main`.

If a proposal is turned down, it moves to `rejected/` with a paragraph
explaining why. Rejected RFCs stay. The next person to have the same idea
deserves to find the reasoning rather than repeat the argument.

## Status is the directory

```text
rfcs/
  2026-08-17-manifest-format-versioning.md   proposed
  accepted/                                  built, tested, on main
  rejected/                                  considered and turned down
```

The frontmatter carries the detail (dates, tracking issue, which release it
landed in). The directory carries the truth. Where the two disagree, the
directory wins, because moving the file is the status change and a field is
only a promise to remember.

**Accepted means shipped, not agreed.** This is deliberate and it is the one
rule worth defending. Projects that mark an RFC accepted at the end of the
discussion accumulate documents describing a database that does not exist, and
readers cannot tell which is which without reading the source. Here, agreement
gets you into `rfcs/`; a working implementation gets you into `accepted/`.

## The document is alive

An RFC is not frozen when it merges. Implementation teaches you things the
design got wrong, and the correction belongs in the document, in the same pull
request as the code that taught you. That is the whole reason these live in
this repository instead of a separate one.

What should not change quietly is the decision. If the design moves far enough
that a reviewer of the original would not recognise it, that is a new RFC
superseding the old one, not an edit.

## Naming

`YYYY-MM-DD-kebab-case-title.md`, dated when the RFC was written. The directory
sorts chronologically and staleness is visible at a glance, which matters more
for design documents than sequential numbering does. This matches the
convention already used for plan documents in `docs/plans/` (see `AGENTS.md`).

## Expected volume

Roughly half a dozen a year. Comparable projects run at that rate for years:
GreptimeDB has about twenty RFCs across three and a half years. If this
directory is growing much faster, the gate above is too wide and we should fix
the gate rather than the documents.
