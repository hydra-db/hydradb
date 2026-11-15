# GQL and Cypher: deviations, extensions and limitations

Every place where the experimental engine's behaviour differs from ISO/IEC
39075:2024 (as Technical Corrigendum 1:2026 amends it), every Cypher
construct it runs that GQL lacks, and every GQL feature it cannot run, with
the decision taken for each.

Sources: the rules ledger (`query-language/gql/tests/engine/gql_features/rules.rs`),
the notes of [`features.md`](features.md), and [`README.md`](README.md).

## How the differences are classified

The engine runs one language, GQL and Cypher 25 together. Two clauses of the
standard sort every difference:

- **§5.3.2.3:** how an implementation treats language that breaks the
  Format or Syntax Rules is implementation-defined (IE005). Accepting such
  language and giving it Cypher's meaning is therefore permitted. A
  conforming GQL query never contains it, so it never sees a difference.
- **§24.5.3:** a conforming query gets the standard's result. Where it does
  not, the difference is a non-conformance; there are five, plus one
  GQLSTATUS detail.

So each difference is one of:

| Kind | Meaning | Count |
|---|---|---|
| Non-conformance | A conforming GQL query gets an answer other than the books'. Kept for Cypher, by choice, and documented | 5, plus one GQLSTATUS detail |
| Extension (IE005) | GQL rejects the text; the engine accepts it with Cypher's meaning | 23 |
| Data model | Defined so that the engine conforms | 1 |
| Cypher syntax with no GQL counterpart | Additional syntax, permitted by §24.5.3 | 8 |
| Limitation | GQL the engine cannot run yet | 25, and 64 optional features |

## Non-conformances

A conforming query gets a different answer, kept because Cypher users rely
on it.

| # | Conforming query | GQL's answer | The engine's | Why |
|---|---|---|---|---|
| N1 | A comparison whose values cannot be compared at runtime, e.g. `WHERE n.x = 1` where `n.x` holds a string (§19.3 GR 2) | `22G04` | Null | Mixed-type properties are common in schemaless data; Neo4j answers null |
| N2 | `DISTINCT`, `UNION`, `EXCEPT` or `INTERSECT` over values that cannot be compared (§22.12 GR 2, §14.2 GR 2) | `22G04` | The values are distinct | As N1 |
| N3 | `sum()` over no values (§20.9 GR 7) | Null | `0` | Neo4j answers `0` |
| N4 | `TRIM` whose character and source cannot be compared at runtime (§20.24 GR 4 and 5; GF05, GF06) | `22G04` | `22G03` | A GQLSTATUS detail of two optional features |
| N5 | Items that do not aggregate beside ones that do, with no `GROUP BY`, e.g. `RETURN i.kind AS k, count(*) AS c` (§14.11 GR 3 b) | Not grouped by `k`: no Syntax Rule forbids the query, and the General Rules give each incoming record its own row | One row per distinct `k`, each with its count | Cypher's implicit grouping; `GROUP BY` is optional (GQ15), so it is how Cypher users group |
| N6 | `MAX` or `MIN` over values that cannot be compared, e.g. a number and a string (§20.9 GR 3 a iii, §19.3 GR 2) | `22G04` | The extreme in the engine's order of types (US007), as `ORDER BY` sorts them | As N1 |

How the engine reads a return whose items all aggregate is in [`README.md`](README.md), "Where the text needs a reading".

Sorting values of different types is not a non-conformance: §16.17 raises
nothing there, and leaves the relative order of incomparable values
implementation-dependent (US007).

## Extensions (IE005)

GQL rejects each of these; the engine accepts it and gives it Cypher's
meaning. Each is listed in the rules ledger as an `Extension`.

| # | Language GQL rejects | GQL rule | What the engine does |
|---|---|---|---|
| X1 | An operand of the wrong static type, e.g. `'a' - 1`, `1 AND true`, `sum('a')`, `upper(1)`, `CAST(1 AS BOOLEAN)` | §19.7, §20.8 Table 4, §20.9, §20.20 to §20.24 Syntax Rules (`42000`) | Raises `22G03` while running rather than `42000` before; with dynamic property types GQL also raises `22G03` at runtime, so only statically known types differ |
| X2 | `CASE` and `COALESCE` results of different types | §22.19 SR 4 | Accepted |
| X3 | A `CASE` whose results are all `NULL` | §20.7 SR 3 | Accepted |
| X4 | `1--1` | §20.21 SR 4 | `1 - -1` |
| X5 | Cypher's abbreviated edges `-->`, `<--`, `--` | §16.7 SR 4, G044 | Accepted beside GQL's `->`, `<-`, `-` |
| X6 | `-9223372036854775808` | §21.2 SR 18 | Read as one value |
| X7 | Unknown escapes such as `'\q'`; control characters in strings | §21.2 SR 31 to 33, 40, 41 | The escape keeps its character; control characters are allowed |
| X8 | Identifier characters outside UAX #31, empty delimited identifiers, identifiers over 16383 characters | §21.3 SR 1 to 5 | The Cypher 25 grammar's rules |
| X9 | Reserved words as names, e.g. `RETURN count(*) AS count`, a variable `type` | §21.3 SR 18 | Accepted |
| X10 | A variable named `unknown` or `session_user` | §21.3 SR 18 | Accepted; the keyword then means the variable |
| X11 | A backtick-quoted variable | §21.1 | Accepted |
| X12 | An item computed from grouping keys, left out of `GROUP BY` | §14.11 SR 10 | Accepted |
| X13 | After `DISTINCT` or an aggregation, `ORDER BY` repeating a projected expression | §14.10 SR 4 | Accepted |
| X14 | A return item that is not a variable, without `AS` | §14.11 SR 8 | Named after its text |
| X15 | An element `WHERE` without a variable | §16.7 SR 21 | Accepted |
| X16 | `SIZE` of a string | §20.22, GF13 | The string's length |
| X17 | `1.5` without GL04 | §21.2 CR 9 | A FLOAT |
| X18 | Cypher's label test `x IS L`, without `LABELED` | §19.9 Format | The same as `x IS LABELED L` |
| X19 | An element `WHERE` inside a selective path pattern that reads a variable from outside it | §16.6 SR 8 | Accepted; the enclosing row supplies the variable |
| X20 | A group variable read where one element is required, e.g. `a.name` after `((a)-[:R]->(b)){2}` | §16.3 SR 16, §20.12 SR 11 | Reads null, as on Cypher's relationship list |
| X21 | Space inside a delimiter such as `-/`, `/->`, `<~` or `~>`, and `4/-2` | §21.3 (delimiter tokens) | Each is read as the characters it is made of, so a separator may stand inside one, and `/-` outside a pattern is a division and a minus |
| X22 | Cypher's type spellings: `x :: T` without `IS`, `T!` for `T NOT NULL`, `PATHS` for `PATH`, `MAP` for `RECORD`, `DURATION` without a unit group, and `ANY<T>` of one component | §19.6 Format, §18.9 Format and CR 51 | The type predicate and the type GQL spells otherwise |
| X23 | Cypher's temporal function names `time`, `datetime`, `localtime` and `localdatetime` | §20.27 Format | The same as `ZONED_TIME`, `ZONED_DATETIME`, `LOCAL_TIME` and `LOCAL_DATETIME` |

## Data model

| # | Construct | Definition |
|---|---|---|
| M1 | `n.id` | Every node has a property `id` whose value is its identity, so `n.id` and `{id: …}` are ordinary property references, and `PROPERTY_EXISTS(n, id)` is true. A stored property of that name is not separate from the identity. An edge's `id` is an ordinary stored property, present only when its writer set one. |

## Cypher syntax with no GQL counterpart

| # | Construct | GQL's nearest construct |
|---|---|---|
| E1 | `UNWIND list AS x` | `FOR x IN list` (GQ10); `UNWIND` also takes a single value as a list of one |
| E2 | `WITH` | `LET`, `FILTER`, `RETURN … NEXT` (GQ09, GQ08, GQ20) |
| E3 | `x IN [list]` | None |
| E4 | `STARTS WITH`, `ENDS WITH`, `CONTAINS` | None |
| E5 | `%` | `MOD(a, b)` (GF01) |
| E6 | `-[:R*1..2]->` | `-[:R]->{1,2}` (G036) |
| E7 | `toUpper`, `toString`, `collect` | `UPPER`, `CAST` (GA05), `COLLECT_LIST` (GF10) |
| E8 | One-argument `log(x)` | `LN(x)` |

Cypher constructs the experimental engine refuses (`0A000`), such as map
literals, list comprehensions, `labels()`, `=~`, `COUNT { }`,
`CREATE` and `MERGE`, run on the legacy executor.

`CALL { }` runs as GQL's inline procedure call (GP01 to GP03), so where the
two languages read it differently the engine reads it as GQL does. With no
variable scope clause it imports every name in scope (§15.2 Syntax Rule 3),
where Cypher 25 imports none: an inner `MATCH (n)` then joins on an outer
`n` instead of binding a new one. `CALL () { }` imports nothing in both.

## Limitations

### In features that run

| # | Area | What is missing | Feature |
|---|---|---|---|
| L1 | Static typing | A type checker over the plan, so that wrong operand types fail before running (X1, X2) | Quality, not conformance (IE005) |
| L2 | Lists | `\|\|` on lists, `TRIM(list, n)` | GV50 |
| L4 | Simplified path patterns | Unscoped concatenation, same-kind unions and multiset alternations share ordinary path execution. A questioned term (G037), mixed set/bag nesting, independent path-valued choice points in one concatenation and repeated multi-edge primaries remain refused (`0A000`) | G039, G080 to G082 |
| L5 | Exact decimals | `1.5` is a FLOAT, as in Cypher (X17); an exact decimal is written with the suffix `M`, `1.5M` (GL05) | GL04 |
| L12 | `NEXT` | Refused (`0A000`) inside a subquery or composite query, and when a part it joins is a composite query; missing primitive results and directly contained `FINISH` in a multi-primary composite are rejected before capability refusal | GQ20 |
| L13 | Quantified edges | Inline properties, a `WHERE` or a label expression other than a type list inside one are refused (`0A000`), as on Cypher's `*1..3` | G036 |
| L14 | Parenthesized paths | Run as the whole path pattern, in unscoped concatenation, questioned (G037, L22), or quantified (G035) when they enclose one edge, including singleton node/edge constraints and predicates with deeper correlated subqueries. Concatenation through a path binding, mode, search prefix or path predicate, repeated multi-edge compositions, iteration-local effects and predicates depending on later bindings remain refused (`0A000`) | G035, G038 |
| L15 | Path search over walks | Under REPEATABLE ELEMENTS, a selective path with an unbounded quantifier and no restrictive path mode is refused (`0A000`): its candidates are walks without end | G016 to G020 |
| L16 | Path pattern unions | Whole-path and unscoped same-kind nesting run; refused (`0A000`): mixed set/bag or scoped nesting, independent choice points in a concatenation, a path search over a union, a variable that is a group in one operand and a single element in another, a variable some operands do not declare that is bound before the `MATCH`, and an operand `WHERE` that reads a later path pattern | G030 to G033 |
| L17 | `CAST` | A list is not cast to a path (`0A000`), and a string cut to a shorter type raises no `01004` warning: an expression cannot raise a completion condition | GA05 |
| L18 | Value types not run yet | Graph reference types (GV60), and Cypher's `POINT`, `VECTOR` and `UUID` are refused (`0A000`) in a type predicate or a `CAST` | |
| L19 | Temporal values, decimals and byte strings | Made in a query only: a parameter holds none of them, and HydraDB stores no such property | GV17, GV35, GV39 to GV41 |
| L20 | Integer range | The engine holds integers from -2^63 to 2^64 - 1: a `CAST` to `INT128`, `UINT128` or wider of a value those types hold but the engine does not is refused (`0A000`) | GV13 to GV16 |
| L21 | String type lengths | A character or byte string type declares lengths of at most 2^24 (16,777,216), since a `CHAR(n)` value is n characters long and a `BINARY(n)` one n bytes; a longer one is refused (`0A000`). A string value has no maximum length (IL013) | GV30, GV31, GV36, GV37 |
| L22 | Questioned paths | Run as the union of the terms each choice of `?` spells (§22.3 GR 3 d), R⁰ ∪ R), so refused (`0A000`) where a union is (L16): inside a union operand, and beyond three `?`s in one path term. A `?` after a quantifier, or over a parenthesized path with a mode, a subpath variable or a union inside, is refused too | G037 |
| L23 | Horizontal aggregation | Runs in a graph pattern's `WHERE` and in `FILTER`, over the group variables of the patterns before it; after a projection the group variables are lists and aggregating one is refused (`42000`). A horizontal aggregate that leaves out a null raises no `01G11` warning: an expression cannot raise a completion condition (as L17). A subquery inside a horizontal aggregate's argument is refused (`0A000`): it would be computed once, before any element is bound. | GE09, GQ17 |
| L24 | `LET` and `PATH[...]` | An aggregate or a subquery inside `LET ... IN ... END` is refused (`0A000`): a subquery is computed before the `LET`, so could not see its names; `PATH[...]` builds a path only from nodes and edges the record binds, and refuses (`0A000`) one it holds only as a reference, such as a collected node | GE03, GE06 |
| L25 | Inline procedure calls | Refused (`0A000`): a call whose query ends in `FINISH` or returns `*`, and Cypher's `IN TRANSACTIONS` | GP01 |
| L26 | Value variable definitions | A declared type, `VALUE x :: INT = 1`, is refused (`0A000`); a definition block before the arms of a composite query is the body's, not an arm's | GP05 |

### Grammar

| # | Area | What is missing |
|---|---|---|
| L6 | `SELECT` | Not in Neo4j's grammar; optional under GQ01 |
| L7 | `--` comments | Cypher reads `--` as an edge (X5); GB02 is optional and not claimed |
| L8 | Parse error positions | `42001` reports no line or column |

### Server and protocol

| # | Area | What is missing |
|---|---|---|
| L9 | Legacy nodes | A node whose `GRAPH_CYPHER_ENGINE` is the legacy executor runs no GQL-only syntax and reports no GQL-status objects |
| L10 | Bolt version | The server negotiates Bolt 5.1 to 5.4, so official drivers ignore the GQL-status fields it sends (they need 5.6 and 5.7) |
| L11 | Data modification | `INSERT`, `SET`, `REMOVE`, `DELETE` (GD01 to GD04) run on the legacy executor only |

### Optional features that do not run

64 of the 234 optional features. "Grammar" is refused with `42001`, because
Neo4j's grammar lacks the syntax and the overlay adds syntax only for what the
engine runs; "engine" parses but is refused with `0A000`; "no query form" has
nothing to probe until a parent feature runs.

| Family | Grammar | Engine | No query form | Notable |
|---|---|---|---|---|
| Graph patterns and predicates (G0xx, G1xx) | 0 | 0 | 0 | — |
| Value types (GV) | 0 | 0 | 2 | Graph and binding table references |
| Query composition (GQ) | 0 | 1 | 1 | `USE`, `FOR` over a binding table |
| Session commands (GS) | 13 | 0 | 5 | Every `SESSION SET`, `RESET` and `CLOSE` form |
| Procedures (GP) | 3 | 1 | 7 | Named procedure calls, binding table and graph variables, `AT` schema |
| Graph types (GG) | 6 | 0 | 5 | Closed graph types, `CREATE GRAPH TYPE` |
| Literals (GL) | 0 | 0 | 0 | — |
| Expressions (GE) | 3 | 0 | 2 | Graph and binding table references, reference parameters |
| Catalog (GC) | 5 | 0 | 0 | `CREATE`/`DROP SCHEMA` and `GRAPH` |
| Functions (GF) | 0 | 0 | 0 | — |
| Data modification (GD) | 1 | 3 | 0 | See L11 |
| Assorted (GA) | 0 | 0 | 0 | — |
| Transactions (GT) | 2 | 0 | 1 | `START TRANSACTION`, `COMMIT`, `ROLLBACK` |
| Lexical (GB) | 1 | 0 | 0 | `--` comments (L7) |
| External objects, undirected edges (GH) | 1 | 0 | 1 | External objects; undirected edges in `INSERT` and graph types |
| **Total** | **35** | **5** | **24** | |

Of the other 170: 164 run, two run in part (GA04, GL04: N1, L5),
GA08 is supported at the diagnostics level, GC00 is claimed, and GA01 and
GG01 are not applicable.

## Still open

- **The larger work:** the type checker (L1), Bolt 5.6 and 5.7 (L10), and
  which optional features come first.
