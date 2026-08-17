---
title: <One line, no trailing punctuation>
status: proposed          # proposed | accepted | rejected | superseded
date: 2026-01-01          # the day the RFC was written
issue: 000                # the tracking issue; "see #NNN", never "closes #NNN"
authors:
  - <github-handle>
implemented_in:           # release tag, added when the file moves to accepted/
supersedes:               # path to an earlier RFC, if any
---

<!--
Copy this file to rfcs/YYYY-MM-DD-kebab-case-title.md and delete the comments
as you fill each section in.

Two rules that make review work:

  Keep each heading to one point. Review happens through inline comments on the
  pull request, and a heading covering three ideas gets one comment covering
  none of them.

  Propose a single solution. Your job is to navigate the problem and come back
  with a recommendation you believe in. List what you rejected under
  Alternatives. A document that presents four options and asks the reader to
  choose moves the design work onto the reviewer.

Sections you genuinely do not need can be dropped, but say so rather than
leaving the heading empty. "Format compatibility: none, this is read-path only"
is a sentence a reviewer needs; a blank section is one they have to chase.
-->

## Summary

<!--
Three to six sentences. What changes, and why it is worth doing. Someone should
be able to read only this section and know whether the rest concerns them.
-->

## Motivation

<!--
The problem, concretely. A query that returns the wrong rows, an operation that
cannot be performed, a failure mode observed in practice, a benchmark number.
Point at issues, traces, or measurements rather than describing the problem in
the abstract.

Then: what happens if we do nothing? Sometimes that is an acceptable answer and
it is better to find that out here.
-->

## Goals and non-goals

<!--
Two short lists. Non-goals are the more useful half: they tell the reviewer
what not to raise, and they keep the implementation from growing while nobody
is watching.
-->

## Design

<!--
The proposal itself, in enough detail that someone else could build it.

Name the types, modules and files you expect to touch. Diagrams and pseudocode
where they carry more than prose would. If the design turns on an invariant,
state the invariant explicitly: this codebase's hardest bugs have been cases
where two components disagreed about one.
-->

## Format compatibility

<!--
Required for anything that touches durable state or a wire protocol. This is
the section that decides whether an operator can upgrade.

  - Which formats change: manifest, WAL, CSC index generation, lease record,
    Bolt, HTTP API.
  - Which format version this bumps, and what a node at the new version does
    when it reads the old one.
  - Whether a node at the old version can read what the new one writes.
  - Whether downgrade is possible, and if not, from exactly which point it
    stops being possible.
  - What happens during a rolling upgrade, while both versions are serving.

"No format change" is a complete and welcome answer. Say it explicitly.
-->

## openCypher and Bolt conformance

<!--
Does this change TCK results? Which scenarios, and in which direction? The
opencypher-tck workflow publishes the current report; name the delta you
expect so the reviewer can check it against what actually lands.

Does it change anything a Neo4j driver observes? Bolt is a compatibility
promise to clients we do not control.
-->

## Operations

<!--
  - Performance: expected effect on query latency, write throughput, index
    build time, memory. Measured if you have a prototype, estimated if not.
    Say which.
  - Memory budgets: anything added to a cache or resident-byte report needs to
    appear in that report.
  - Observability: new metrics, spans, or log fields, and what an operator is
    meant to do when one of them moves.
  - Configuration: new settings, their defaults, and why those defaults are
    safe for someone who never reads this document.
-->

## Testing

<!--
How we will know it works, and how we will know when it stops working.

  - Unit and integration coverage.
  - Failure-oriented tests: the test that fails today because of the bug, or
    that would fail if the invariant broke.
  - Chaos or fencing harnesses where the change touches writer ownership,
    leases, or snapshot visibility.
  - Benchmarks, where the motivation was performance.
-->

## Rollout

<!--
How this reaches users without breaking anyone.

Phases, feature flags, and the name the feature carries while it is unstable.
Mark experimental surfaces in the name itself rather than only in
documentation, so someone who never reads the release notes still sees it.

If the change is not reversible once data has been written under it, say so
here in as many words.
-->

## Alternatives

<!--
What else was considered, and the specific reason each was rejected. Include
the option of doing nothing.

This is the section that pays for itself. It is what stops the same idea being
re-proposed in a year, and it is the first thing to read when the chosen design
turns out to be wrong.
-->

## Open questions

<!--
What is genuinely unresolved, and who or what would resolve it. An RFC can
merge with open questions; it cannot merge with open questions in the Design
section.
-->

## Updates

<!--
Append as the implementation teaches you things. Date each entry.

  - 2026-01-15: overlay merge was O(frontier^2) as specified; revised to ...
-->
