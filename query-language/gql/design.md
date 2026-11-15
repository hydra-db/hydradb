---
title: ISO GQL mandatory features in the experimental engine
status: step-5-complete
date: 2026-10-17
branch: vishal/iso-gql-mandatory
base_commit: 35e9fc94
tags:
  - gql
  - experimental-engine
  - conformance
  - diagnostics
---

# ISO GQL mandatory features in the experimental engine

Goal: the experimental engine supports every mandatory feature of ISO/IEC
39075:2024 as amended by Technical Corrigendum 1:2026, at four levels:
parsing, lowering and semantic checks, execution, and diagnostics. It does so
through the existing Cypher 25 front end and without changing the behaviour of
any Cypher it already supports.

## Sources

- **Ground truth:** ISO/IEC 39075:2024, *Information technology — Database
  languages — GQL*, and its Technical Corrigendum 1, ISO/IEC 39075:2024/Cor
  1:2026. Both are published by ISO. Cite them by clause number; never copy
  their text here.
  - The mandatory surface is everything in Clauses 6–23 not gated by a
    Conformance Rule (§24.2; Annex A lists the rules). Clauses 22 and 23 have
    rules too: universal comparison (GA04), comparison of paths (GA09),
    group-variable operations (GQ17) and diagnostic records (GA08) are
    optional.
  - A few Conformance Rules forbid syntax outright, because it is a
    specification device rather than user syntax: `TEMP` element variables
    (§16.7), `NO BINDINGS` (§14.11, §16.3) and `GROUP` list types (§18.9).
  - The mandatory surface is derived from the grammar, not listed by hand:
    every production of Clauses 6–21, with each Conformance Rule removing the
    production or alternative it forbids, the Syntax Rules that tie a
    production to a removed one (§9.1 Syntax Rules 5 and 6, §13.1 Syntax
    Rule 5), and what remains reachable from `<GQL-program>`. 205 productions
    remain, besides single characters.
    `query-language/gql/tests/engine/gql_features/grammar.rs` lists them with the
    matrix row that covers each, and its header gives the method.
  - Read literally, the rules make `SELECT` optional: `<select statement>`
    occurs only inside `<focused linear query statement>`, which §14.3 gates
    behind GQ01 whether or not a `USE` clause is written. That looks
    unintended, but the text is unambiguous, so the matrix follows it.
    `README.md` lists the other constructs the rules make optional.
  - The corrigendum adds gates for `SESSION SET SCHEMA` (GS09),
    `SESSION SET GRAPH` (GS17), `SESSION CLOSE` (GS18), `FINISH` (GQ27),
    byte-string literals (GV35), and `LET VALUE` (GP05).
  - It replaces the §24.2 item 1 claim with "GC00 or GC04" and moves property
    reference type errors to `22G03`.
  - It requires grouping and aggregating return items to be disjoint (§14.11),
    tightens aggregates in sort keys (§14.10), and reserves `TYPE`.
  - Two slips in it: it removes `TYPE` from the *pre-reserved* list, but the
    2024 text has `TYPE` among the non-reserved words. And Table 8 is not
    amended for the new `2202E`, `2202F`, `2202M` and `2201Z` codes (all
    optional-feature codes).
- **Reference only:** Neo4j's GQL conformance appendix
  (`neo4j.com/docs/cypher-manual/current/appendix/gql-conformance/`). It is used
  to spot where a GQL rule would change Cypher behaviour users rely on. It does
  not define the target: it counts optional features (GD01, GQ12/13) as
  mandatory, and omits `SELECT`.
- **Grammar:** the vendored Cypher 25 grammar that
  `crates/cypher-parser-antlr/build.rs` compiles matches upstream
  `neo4j/cypher-language-support@ce3820b4` (Neo4j 2026.08.0). Upstream main is
  `29069e21` (2026.10), which adds map comprehensions, `FLOAT16`/`BFLOAT16` and
  admin-command changes, and nothing GQL-related.
- **Engine:**
  - `crates/cypher-parser-antlr/src/lower.rs`: lowers the typed ANTLR tree
    to the shared AST. Before this work it validated the tree, then
    re-tokenized the text with a hand parser covering a slice of Cypher.
  - `crates/cypher-engine/src/plan/{lower,logical}.rs` and `src/execute.rs`.

## Rules for resolving conflicts

1. **The standard decides what is mandatory and how a new construct behaves.**
2. **Supported Cypher does not change, and the engine runs one language.**
   Where GQL and Cypher meet, two clauses of the standard classify the
   difference:
   - Language GQL rejects (a Syntax Rule or Conformance Rule fails) may be
     accepted with Cypher's meaning: §5.3.2.3 leaves its treatment
     implementation-defined (IE005). That is an **extension**, and a
     conforming query never meets one: reserved words as names, items
     without `AS`, Cypher's `-->`.
   - A **conforming** query that gets an answer other than the books' is a
     **non-conformance** (§24.5.3). There are five, kept by choice:
     incomparable values compare to null rather than raising `22G04`,
     `DISTINCT` and the set operators treat them as distinct, `sum()` over
     nothing is `0`, items that do not aggregate group the result
     without `GROUP BY`, and `MIN`/`MAX` over incomparable values use the
     engine's order of types rather than raising `22G04`.
     [`deviations.md`](deviations.md) records each, plus the GQLSTATUS detail
     of a runtime-incomparable `TRIM` character.
3. **Constructs the engine does not yet execute follow the standard,** even
   where Neo4j's Cypher differs. Examples: `TRIM` removes spaces only (§20.24),
   and an unquoted `TRUE`/`FALSE`/`UNKNOWN` after `IS` is a truth test, because
   they are GQL reserved words, while a backticked name is a label. In the same
   way an unquoted `e IS DIRECTED` is the directed predicate, and `XOR` shares
   the level of `OR`.
4. **Diagnostics are additive.** Every outcome gets a GQLSTATUS (§4.9, §23)
   alongside the existing `QueryFailureReason` metric buckets.
5. **The grammar is Neo4j's, plus an overlay.** GQL syntax it lacks goes in
   `query-language/gql/overlays/`, which `build.rs` applies to its
   staged copy; the vendored `.g4` files stay byte for byte Neo4j's. An
   overlay item only adds: text Cypher 25 parses keeps its parse. Optional
   syntax goes in only once the engine runs it.

## Starting point

Before this work, the experimental engine executed the Cypher core of the
mandatory surface: `MATCH`, `OPTIONAL MATCH`, `RETURN`, comparisons,
`IS NULL`, integer arithmetic, basic `CASE` and the aggregates. Its parser
adapter re-tokenized the text and typed only that slice, so a comment or a
trailing `;` made a query untyped.

What was missing, and what each construct's status is now, is in
[`features.md`](features.md). That matrix is the record:
`query-language/gql/tests/engine/gql_features.rs` generates it from probe queries
run against the engine, so this document does not repeat it.

## Steps

1. **Front end: lower from the typed ANTLR tree.** Replace the hand
   re-tokenizer in `lower.rs` with a walk of the generated contexts. First
   reach parity with what lowered before, guarded by `query-language/shared/tests/parser/cypher25.rs` and
   the engine suites; then add the missing mandatory constructs. The shared AST grows only where it
   must: label-expression trees, `EXISTS`, normal-form and truth predicates,
   extended CASE, standalone order clauses, nested queries.
2. **Semantics in engine lowering.** Scope and kind checks, aggregate placement,
   `ORDER BY` reference rules, and the corrigendum's §14.10/§14.11 rules. Each
   failure gets a GQLSTATUS.
3. **Execution:** float arithmetic, then the functions, predicates, label
   expressions and subqueries, each tested against the standard's rules.
4. **Diagnostics:**
   - A `GqlStatus` catalog in `hydradb-cypher-ast`.
   - Mappings from parse, lowering, planning and evaluation errors to codes.
   - Success statuses on results.
   - Warnings.
5. **Conformance docs:** `query-language/gql/features.md` (the matrix, generated by
   `query-language/gql/tests/engine/gql_features.rs` from probe queries),
   `query-language/gql/deviations.md` (every difference from the books), `query-language/gql/AGENTS.md`
   (how to contribute), and `query-language/gql/README.md` (claims, implementation-defined
   choices and the GQLSTATUS mapping).

## Outcome

Steps 1 to 5 are done. What is supported is in [`features.md`](features.md);
every difference from the books is in [`deviations.md`](deviations.md), and
the implementation-defined choices in [`README.md`](README.md).

- **Step 1:** `lower.rs` walks the typed ANTLR tree. The surface typed before lowers
  unchanged; the existing parser, engine and server suites pass.
- **Steps 2 to 4:** label expressions, truth tests, the normalized predicate,
  `UNKNOWN`, extended `CASE`, `NULLIF`, `||`, string functions, float
  arithmetic, numeric functions, `EXISTS` as a mark join, standalone
  `ORDER BY`, nested queries, and GQLSTATUS on every outcome and error.
  The Syntax Rules and raised conditions of the mandatory subclauses are
  listed one by one in the rules ledger (`query-language/gql/tests/engine/gql_features/rules.rs`), each held
  by a conformance test, recorded as an extension or a deviation, or
  explained as unable to arise here.
- **Grammar overlay:** `EXISTS ( pattern )`, double-quoted identifiers and
  aliases, doubled quotes in strings, and the optional G002, G006, G007, G012, G044, G112, GA03,
  GL03, GL11, GQ02, GQ04 to GQ07, GQ11, GQ18, GQ21 and GQ24 syntax, through
  `query-language/gql/overlays/`.
- **`n.id`:** defined as a property every node has, whose value is its
  identity, so `n.id` is an ordinary property reference. An edge's `id` is the
  property its writer stored, if any.
- **`SESSION_USER`:** the server passes the transport principal in
  `QueryContext::session_user`, and the engine binds it per request
  (`LoweredTemplate::bind_as`), never into a cached template or from a
  caller's parameters.
- **GQL-status objects on the wire:** `GraphError::GqlQuery` carries an
  engine error's object through the query transport to Bolt FAILURE and the
  HTTP error body; `QueryResultSet::statuses` carries an outcome's to Bolt
  SUCCESS and the HTTP response.
- **Mandatory gaps left:** none. Reserved words are also accepted as names,
  an extension (IE005). Five non-conformances remain, by choice
  ([`deviations.md`](deviations.md)).

## Open questions

- Refresh the vendored grammar to Neo4j 2026.10: when?

## Verification

- `just test-cypher`: the shared AST and the parser, including
  `query-language/shared/tests/parser/cypher25.rs`.
- `just test-cypher-engine`: Clippy `-D warnings` and the engine tests,
  including `query-language/gql/tests/engine/gql_conformance.rs` (behaviour and
  GQLSTATUS) and `query-language/gql/tests/engine/gql_features.rs` (the matrix).
  The matrix test holds every running probe to the result the books give, every mandatory production to a row
  (`query-language/gql/tests/engine/gql_features/grammar.rs`), and every rule of the rules ledger
  (`query-language/gql/tests/engine/gql_features/rules.rs`) to a test, an extension, a deviation, a reason it
  cannot arise, or a recorded gap.
- `just test-experimental-cypher`: the root package with the server features,
  whenever a plan or expression variant changes.
