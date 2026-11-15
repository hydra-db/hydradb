# Contributing GQL support

This directory tracks ISO GQL support in the experimental engine. Read this
before adding a GQL feature, mandatory or optional, or changing how one
behaves. It applies to people and to coding agents alike.

| File | What it holds |
|---|---|
| [`features.md`](features.md) | Every mandatory construct and optional feature, and its status at four levels. Generated; start here to pick something to work on. |
| [`README.md`](README.md) | Conformance claims, a summary of the differences from the books, implementation-defined choices, the GQLSTATUS mapping, open work. |
| [`design.md`](design.md) | Why it is built the way it is, and the rules for resolving conflicts with Cypher. |
| [`deviations.md`](deviations.md) | Every difference from the books: the non-conformances, the extensions GQL permits (IE005), Cypher-only syntax and limitations, with the decision for each. |
| [`../cypher/grammar/`](../cypher/grammar/) | The unchanged Neo4j Cypher 25 grammar snapshot consumed by the Rust parser build. |
| [`overlays/`](overlays/) | Additive GQL grammar overlays applied only to staged build copies. |
| [`../shared/scripts/`](../shared/scripts/) | Public-protocol A/B, compatibility, EXPLAIN, and observability harnesses for the experimental engine. |
| [`tests/`](tests/) | Standalone GQL conformance and feature-ledger tests. |
| [`../shared/tests/`](../shared/tests/) | Shared engine integration tests, fixtures, and parser-lowering tests. |

## Ground truth

- **Normative text:** ISO/IEC 39075:2024 and its Technical Corrigendum 1:2026.
  When the two disagree, the corrigendum wins.
- **Cite, don't copy:** cite the standard by clause number (`§20.24 General
  Rule 4`). Do not copy its text into this repository: it is licensed, not
  public.
- **Neo4j's GQL appendix is a reference only.** It shows where Cypher users
  already rely on a behaviour. It does not decide what GQL requires, and it
  counts several optional features as mandatory.
- **Mandatory means** everything in Clauses 6 to 23 that no Conformance Rule
  gives a feature ID (§24.2). An optional feature is one with a feature ID
  (Annex A lists the rules, Annex D the IDs). Read the Conformance Rules of
  every subclause a construct touches: a rule in one subclause can gate syntax
  defined in another. For example, §14.10 makes any sort key other than a
  variable optional (GQ14), and §21.2 makes `1.5` optional (GL04; mandatory
  GQL spells it `1.5E0`).
- **Some syntax is not syntax at all.** A Conformance Rule with no feature
  forbids it outright, because it exists only to define something else:
  `TEMP` element variables, `NO BINDINGS`, `GROUP` list types. Do not list or
  implement these.

## When GQL and Cypher disagree

1. If the engine already executes the construct as Cypher, its behaviour does
   not change. Then decide which kind of difference it is
   ([`deviations.md`](deviations.md) has every one):
   - GQL rejects the text and the engine accepts it: an **extension**,
     permitted by §5.3.2.3 (IE005). Record it as `Extension` in `rules.rs`.
   - A conforming GQL query gets a different answer: a **non-conformance**
     (§24.5.3). Record it as `Deviation` in `rules.rs` and in
     `deviations.md`, with the reason it is kept; there are five, plus one
     GQLSTATUS detail.
2. If the engine does not execute the construct yet, implement the standard's
   behaviour, even where Neo4j's Cypher differs.
3. The grammar is Neo4j's Cypher 25 grammar, vendored unmodified, plus the
   GQL overlay in `query-language/gql/overlays/`. Add GQL syntax
   there (`//@ replace`, `//@ extend` or `//@ insert-after` a named rule, with
   the subclause it implements), never in the vendored `.g4` files. An item
   must only add: run `just test-cypher-engine` and `just test-experimental-cypher`
   to show no Cypher query changes its parse. Add optional syntax only when
   the engine will run it; syntax for a feature that is not claimed is not
   part of the language, and `42001` is the right answer for it.

## Where each level lives

| Level | Code | What to do there |
|---|---|---|
| Parser | `crates/cypher-parser-antlr/src/lower.rs` | Walk the ANTLR tree (`Cst`, `Child`) into the shared AST. Type a construct only once the engine executes it; until then `self.refuse()`, so it stays an untyped fragment in the bucket of the clause it sits in. |
| AST | `crates/cypher-ast/src/{query,expression}.rs` | Add a node only when no existing one says the same thing. An AST change breaks every exhaustive match below; the compiler lists them. |
| Semantics | `crates/cypher-engine/src/plan/lower.rs`, `plan/planner.rs` | Scope (`check_scope`, `validate_declared_bindings`), Syntax Rules, the lowering to logical and physical plans. |
| Execution | `crates/cypher-engine/src/execute.rs`, `src/execute/functions.rs` | General Rules. New scalar semantics go in `functions.rs`; plan operators in `execute.rs`. |
| Diagnostics | `crates/cypher-ast/src/status.rs` | Every condition is a `GqlStatus` constant from Table 8 (and Corrigendum 1). Raise one with `EngineError::data(GqlStatus::…, message)` at execution, `GraphPlanError::syntax`, `invalid_reference` or `data` at planning, and `GraphRead::raise_completion_condition` for a warning. |

Diagnostics rules:

- Use the condition the standard names in the General Rule you implement.
  Never invent a standard-defined code.
- A condition the standard does not name goes in an implementation-defined
  class or subclass (§23.1). One is used today: `50000`, for failures that are
  not the query's (storage, limits, cancellation).
- Valid syntax the engine does not execute is `0A000`, feature not supported.
  That is a standard-defined class GQL takes from ISO/IEC 9075, not an
  implementation-defined one.
- Exceptions use the `Exception` category. A warning or no-data condition is a
  completion condition; it never fails the query.

Adding a variant to `GraphLogicalPlan`, `GraphPhysicalPlan`,
`LogicalExpression` or `PhysicalExpression` also breaks exhaustive matches in
the root package (`src/shard/experimental_cypher/statistics.rs`). Those
compile only with the server features, so run `just test-experimental-cypher`
before you push.

## The GQL test suite

| Test file | What it pins |
|---|---|
| `query-language/gql/tests/engine/gql_conformance.rs` | Behaviour: results and the GQLSTATUS of every exception, one section per subclause (`// ---- §20.24 … ----`). |
| `query-language/gql/tests/engine/gql_features.rs` | The matrix: the `FEATURES` table (ID, name, probe query, note) that `features.md` is generated from. It fails when the committed `features.md` is not what the table and the engine render. |
| `query-language/gql/tests/engine/gql_features/grammar.rs` | The mandatory grammar: every production minimum conformance allows, and the one mandatory row that covers it. The test fails if a production names a row that is missing or optional. |
| `query-language/gql/tests/engine/gql_features/rules.rs` | The rules ledger: every Syntax Rule restriction and raised condition of Clauses 6 to 23, mandatory and optional, each with its row and how it is held (a conformance test, an extension, a deviation, not applicable, or a gap). The test fails if a named test or row does not exist, and derives each running row's Diagnostics cell from it. |
| `query-language/shared/tests/parser/cypher25.rs` | Lowering: the AST a query produces, and the failure bucket of constructs that are not typed (`constructs_without_an_executor_stay_untyped`). |

### The mandatory rows

The mandatory rows are derived from the standard, not chosen: `grammar.rs`
lists every production of the mandatory grammar, and its header explains how
that list was derived from the Format sections and Conformance Rules. So:

- To split a mandatory row, add the new row and move the productions it
  exercises onto it in `grammar.rs`.
- Never remove a mandatory row without moving its productions to another
  row.
- A mandatory row with no productions is fine when it pins an alternative,
  such as `RETURN *` or `RETURN ALL`.
- When a corrigendum or a new edition changes the grammar or a Conformance
  Rule, rerun the derivation against the new text and update `grammar.rs`.

The two engine files run their queries against the compat graph in
`query-language/shared/tests/engine/common/compat.rs`; add any data a new test needs
there. The parser tests lower text only and need no graph.

## Adding or changing a feature

1. **Pick a row** in `features.md` and read its subclause, including the
   Syntax Rules, General Rules and Conformance Rules. Read the corrigendum
   for the same subclause.
2. **Write the tests first** in `gql_conformance.rs`, under that subclause's
   section:
   - every result the General Rules define, including null inputs;
   - every exception, asserting its `GqlStatus` with the `status(query)` helper;
   - every Syntax Rule the feature's syntax can break, asserting `42000`
     (or `42001`, `42002`).

   Then find each of those rules in `rules.rs` (every rule of Clauses 6 to 23
   is there; an unimplemented feature's rules are `Pending`) and set how it
   is held: `Test` with the test that pins it; `NotApplicable` with the
   reason it cannot arise; `Extension` when the engine also accepts text the
   rule forbids; `Deviation` when a conforming query gets another answer
   (also listed in `deviations.md`); `Gap` when it should hold
   and does not. The matrix test fails when a row runs while one of its rules
   is still `Pending`, or when a running row has no rules at all: a construct
   whose subclause has none still gets a `NotApplicable` entry saying so.
3. **Implement it level by level**, parser to execution, as in the table
   above. Remove the construct from `constructs_without_an_executor_stay_untyped`
   if it was listed there.
4. **Update the matrix.** Never edit `features.md` by hand: it is generated.
   Edit the feature's entry in the `FEATURES` table in `gql_features.rs`:
   - `probe`: a query that uses the feature (a new feature needs one). A
     mandatory row's probe uses only mandatory syntax, so that it cannot pass
     on the strength of an optional feature.
   - `expect`: the probe's result as the General Rules give it on the compat
     graph, worked out from the books, never copied from the engine. A probe
     that runs without one, or returns something else, fails the test.
   - `diagnostics`: leave it `Diagnostics::FromProbe`. A refused row shows the
     code it is refused with; a running row shows what `rules.rs` says of it.
   - `levels` or `Diagnostics::Judged`: only for a cell a probe cannot decide
     (➖ partial, — not applicable).
   - `note`: only when it says something the cells cannot. There are four
     kinds, and the test enforces where each may go:
     - `Note::Deviation`: a conforming query gets an answer other than the
       standard's (§24.5.3). Allowed on any row.
     - `Note::Extension`: the engine also accepts language the standard
       rejects, with Cypher's meaning (§5.3.2.3). Allowed on any row.
     - `Note::Limit`: supported, except where the note says. Allowed only on a
       row whose Execution is ✅.
     - `Note::Reason`: why a hand-set mark or a claim. Required on such a row
       (a `Deviation` or `Extension` also satisfies it), and allowed nowhere
       else.

     Do not restate a cell ("not in the grammar" is what `42001` already
     says), describe how a feature is lowered, or list its GQLSTATUS codes
     (that is the GQLSTATUS mapping in `README.md`).

   Then regenerate and commit the result:

   ```bash
   GQL_FEATURES_WRITE=1 just test-cypher-engine --test gql_features
   ```

   The Parser, Semantics, Execution and refusal cells, and the summary at the
   top, come from running the probes; you never set them.
5. **Update `deviations.md`** when the change adds an extension or a
   non-conformance or closes a limitation, and **`README.md`** when it makes
   an implementation-defined choice.
6. **Run the suites:**

   ```bash
   just test-cypher             # shared AST and parser
   just test-cypher-engine      # engine, Clippy -D warnings
   just test-experimental-cypher  # root package with the server features
   ```

## Before a pull request

- [ ] `features.md` was regenerated from the `FEATURES` table.
- [ ] Every new rule of the subclauses you touched is in `rules.rs`, and
      every running probe has the expected result the books give.
- [ ] No Cypher query that worked before changed its result. The engine's
      existing tests are the guard, so do not edit one to make it pass;
      record an extension or a non-conformance instead.
- [ ] Exceptions use the standard's GQLSTATUS; anything else uses an
      implementation-defined code, documented in `README.md`.
- [ ] No text from the standard was copied; no local paths or private
      material are in the docs.
