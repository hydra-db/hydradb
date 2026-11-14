//! Generates `query-language/gql/features.md`, the GQL feature matrix, and holds it to the
//! engine.
//!
//! [`FEATURES`] is the one place the matrix is written: every mandatory
//! construct and every optional feature of ISO/IEC 39075:2024 (Annex D, with
//! the feature IDs Technical Corrigendum 1:2026 adds), each with a probe query
//! that uses it and a note. The test runs every probe through the stages a
//! query passes and renders the whole file from the verdicts:
//!
//! - **Parser**: the Cypher 25 grammar accepts the text *and* lowering types
//!   it into the shared AST.
//! - **Semantics**: the engine's lowering and planning accept it.
//! - **Execution**: it runs against the compat test graph and returns
//!   [`Feature::expect`], the answer the standard's General Rules give.
//! - **Diagnostics**: the GQLSTATUS a refused feature is refused with; for a
//!   feature that runs, what [`rules::RULES`] says of its row: ✅ when every
//!   condition the books define for it is pinned by a test.
//!
//! The mandatory rows are not a hand-picked list: every production of the
//! standard's grammar that minimum conformance allows is listed in
//! [`grammar::MANDATORY_GRAMMAR`] with the one row that covers it, and the
//! test holds the two together.
//!
//! A cell the probe cannot decide -- partial support, not applicable -- is
//! set by hand in [`Feature::levels`] or
//! [`Diagnostics::Judged`], and the note says why.
//!
//! The rendered file is compared with the committed one, so the matrix cannot
//! drift from the engine. After changing either, regenerate it:
//!
//! ```text
//! GQL_FEATURES_WRITE=1 just test-cypher-engine --test gql_features
//! ```

#[path = "../../../shared/tests/engine/common/mod.rs"]
mod common;
#[path = "gql_features/grammar.rs"]
mod grammar;
#[path = "gql_features/rules.rs"]
mod rules;

use std::fmt::Write as _;

use common::compat::{graph, show};
use grammar::MANDATORY_GRAMMAR;
use hydradb_cypher_ast::Statement;
use hydradb_cypher_engine::{CypherEngine, GqlStatus, ListParameterMap, ParameterMap, ScalarValue};
use hydradb_cypher_parser_antlr::{parse_cypher25, parse_cypher25_syntax};
use rules::{Coverage, Rule, RULES};

/// How a row's Diagnostics cell is decided.
#[derive(Clone, Copy)]
enum Diagnostics {
    /// The code the probe is refused with; when it runs, what the rules
    /// ledger says of the row (see [`ledger_mark`]).
    FromProbe,
    /// Set by hand; the note says why.
    Judged(&'static str),
}

/// One row of the matrix.
struct Feature {
    /// A clause number for a mandatory construct, a feature ID otherwise.
    id: &'static str,
    name: &'static str,
    /// A query that uses the feature; `None` when it has no query form here.
    probe: Option<&'static str>,
    /// The result the standard's General Rules give for the probe on the
    /// compat graph, worked out from the books rather than copied from the
    /// engine: rows separated by `; `, values by `, `, each as
    /// `common::compat::show` spells it (`"a"`, `3`, `2.5f`, `null`,
    /// `#1000`). Rows are compared in order when the probe has `ORDER BY`,
    /// sorted otherwise. Required for a probe that runs; a probe that does
    /// not run yet may carry it for when it does.
    expect: Option<&'static str>,
    /// Parser, Semantics and Execution set by hand rather than by the probe.
    levels: Option<[&'static str; 3]>,
    diagnostics: Diagnostics,
    /// A conformance claim rather than something a query exercises.
    claimed: bool,
    note: Note,
}

/// What a row's note says. A row has a note only when it says something its
/// cells cannot, and the kind decides where it may appear (see
/// [`check_note`]).
#[derive(Clone, Copy)]
enum Note {
    None,
    /// A conforming query gets an answer other than the standard's (§24.5.3).
    /// Allowed on any row.
    Deviation(&'static str),
    /// The engine also accepts language the standard rejects, with Cypher's
    /// meaning (§5.3.2.3, IE005); conforming queries are unaffected. Allowed
    /// on any row.
    Extension(&'static str),
    /// Supported, except where this says. Allowed only on a row that runs.
    Limit(&'static str),
    /// Why a hand-set mark or a claim. Required on such a row, and allowed
    /// nowhere else.
    Reason(&'static str),
}

impl Note {
    fn render(self) -> String {
        match self {
            Self::None => String::new(),
            Self::Deviation(text) => format!("Deviation: {text}"),
            Self::Extension(text) => format!("Extension: {text}"),
            Self::Limit(text) => format!("Limit: {text}"),
            Self::Reason(text) => text.to_string(),
        }
    }
}

/// The note rules, given a row's rendered cells. A hand-set mark or a claim
/// needs a reason, a deviation or an extension to explain it; a reason explains nothing
/// else; a limit qualifies only a feature that runs.
fn check_note(feature: &Feature, cells: &Cells) -> Result<(), String> {
    let judged = feature.levels.is_some() || matches!(feature.diagnostics, Diagnostics::Judged(_)) || feature.claimed;
    match (feature.note, judged) {
        (Note::Reason(_) | Note::Deviation(_) | Note::Extension(_), true) => Ok(()),
        (_, true) => Err(format!(
            "{} sets a mark by hand and needs a Reason, Deviation or Extension note",
            feature.id
        )),
        (Note::Reason(_), false) => Err(format!("{} has a Reason note but no hand-set mark to explain", feature.id)),
        (Note::Limit(_), false) if cells.levels[2] != "✅" => Err(format!("{} has a Limit note but does not run", feature.id)),
        _ => Ok(()),
    }
}

/// Mandatory constructs first, by clause; then every optional feature.
#[rustfmt::skip]
const FEATURES: &[Feature] = &[
    Feature { id: "6", name: "GQL-program: one query, no transaction commands", probe: Some("MATCH (n:Hub) RETURN n.name AS name"), expect: Some("\"hub\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "14.3", name: "A result statement on its own", probe: Some("RETURN 1 AS v"), expect: Some("1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "14.3 sequence", name: "Statements in sequence", probe: Some("MATCH (a:Node {name: 'a'}) MATCH (a)-[:R]->(b) RETURN b.name AS name"), expect: Some("\"b\"; \"c\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "14.3 nested", name: "Nested query specification `{ … }` as a query", probe: Some("{ MATCH (n:Hub) RETURN n.name AS name }"), expect: Some("\"hub\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "14.4", name: "`MATCH`", probe: Some("MATCH (n:Hub) RETURN n.name AS name"), expect: Some("\"hub\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "14.4 OPTIONAL", name: "`OPTIONAL MATCH`", probe: Some("MATCH (n:Hub) OPTIONAL MATCH (n)-[:R]->(m) RETURN n.name AS a, m.name AS b"), expect: Some("\"hub\", null"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "14.9", name: "`ORDER BY` statement", probe: Some("MATCH (n:Hub) ORDER BY n RETURN n.name AS name"), expect: Some("\"hub\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "14.10", name: "`ORDER BY` after `RETURN`", probe: Some("MATCH (i:Item) RETURN i.score AS s ORDER BY s"), expect: Some("0; 1; 2; 3; 5; null"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "14.11", name: "`RETURN` items with `AS`", probe: Some("MATCH (i:Item) RETURN i.kind AS k, i.score AS s"), expect: Some("\"fruit\", 0; \"fruit\", 1; \"fruit\", 3; \"fruit\", null; \"veg\", 2; \"veg\", 5"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Extension("an item that is not a variable may also omit `AS`, which §14.11 Syntax Rule 8 requires; the column is then named after its text.") },
    Feature { id: "14.11 DISTINCT", name: "`RETURN DISTINCT`", probe: Some("MATCH (i:Item) RETURN DISTINCT i.kind AS k"), expect: Some("\"fruit\"; \"veg\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Deviation("values that cannot be compared are distinct instead of raising `22G04`.") },
    Feature { id: "14.11 ALL", name: "`RETURN ALL`", probe: Some("MATCH (i:Item) RETURN ALL i.kind AS k"), expect: Some("\"fruit\"; \"fruit\"; \"fruit\"; \"fruit\"; \"veg\"; \"veg\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "14.11 *", name: "`RETURN *`", probe: Some("MATCH (n:Hub) RETURN *"), expect: Some("#1010"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "16.4", name: "Path pattern lists, joined on shared variables", probe: Some("MATCH (a:Node)-[:R]->(b), (b)-[:R]->(c) RETURN count(*) AS n"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "16.4 WHERE", name: "Graph pattern `WHERE`", probe: Some("MATCH (a:Node) WHERE a.name = 'a' RETURN a.name AS n"), expect: Some("\"a\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "16.7", name: "Node patterns", probe: Some("MATCH (n) RETURN count(*) AS c"), expect: Some("12"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "16.7 ->", name: "Edge pointing right `-[ ]->`", probe: Some("MATCH (a)-[e:R]->(b) RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "16.7 <-", name: "Edge pointing left `<-[ ]-`", probe: Some("MATCH (a)<-[e:R]-(b) RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "16.7 -", name: "Edge in either direction `-[ ]-`", probe: Some("MATCH (a)-[e:R]-(b) RETURN count(*) AS c"), expect: Some("11"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "16.7 label", name: "Element label test with `:` or `IS`", probe: Some("MATCH (a IS Node)-[e:R]->(b:Node) RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "16.7 WHERE", name: "Element pattern `WHERE`", probe: Some("MATCH (a:Node WHERE a.name = 'a')-[e:R WHERE e IS NOT NULL]->(b) RETURN count(*) AS c"), expect: Some("2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "16.7 {}", name: "Element property specification `{k: v}`", probe: Some("MATCH (a:Node {name: 'a'})-[:R]->(b) RETURN count(*) AS c"), expect: Some("2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "16.8 |", name: "Label disjunction `|`", probe: Some("MATCH (n:Hub|Item) RETURN count(*) AS c"), expect: Some("7"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "16.8 &", name: "Label conjunction `&`", probe: Some("MATCH (n:Node&Node) RETURN count(*) AS c"), expect: Some("5"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "16.8 !", name: "Label negation `!`", probe: Some("MATCH (n:!Item) RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "16.8 ( )", name: "Parenthesized label expressions", probe: Some("MATCH (n:(Hub|Node)&!Item) RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "16.17", name: "`ASC` `ASCENDING` `DESC` `DESCENDING`", probe: Some("MATCH (i:Item) RETURN i.score AS s, i.name AS n ORDER BY s DESCENDING, n ASC"), expect: Some("null, \"Durian\"; 5, null; 3, \"apple\"; 2, \"carrot\"; 1, \"banana\"; 0, \"apple\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "19.3", name: "Comparison `=` `<>` `<` `>` `<=` `>=`", probe: Some("RETURN 1 = 1 AS a, 1 <> 2 AS b, 1 < 2 AS c, 2 > 1 AS d, 1 <= 1 AS e, 1 >= 1 AS f"), expect: Some("true, true, true, true, true, true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Deviation("incomparable values compare to null instead of raising `22G04`.") },
    Feature { id: "19.4 { }", name: "`EXISTS { graph pattern }`", probe: Some("MATCH (n:Node) WHERE EXISTS { (n)-[:R]->() } RETURN count(*) AS c"), expect: Some("5"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "19.4 ( )", name: "`EXISTS ( graph pattern )`", probe: Some("MATCH (n:Node) WHERE EXISTS ((n)-[:R]->()) RETURN count(*) AS c"), expect: Some("5"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "19.4 query", name: "`EXISTS { query }`", probe: Some("MATCH (n:Node) WHERE EXISTS { MATCH (n)-[:R]->(m) RETURN m } RETURN count(*) AS c"), expect: Some("5"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "19.5", name: "`IS [NOT] NULL`", probe: Some("RETURN null IS NULL AS v, 1 IS NOT NULL AS w"), expect: Some("true, true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "19.7", name: "`IS [NOT] [form] NORMALIZED`", probe: Some("RETURN 'a' IS NFC NORMALIZED AS v, 'a' IS NOT NORMALIZED AS w"), expect: Some("true, false"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.1 references", name: "Node and edge references as values", probe: Some("MATCH (a:Node)-[e:R]->(b) RETURN a, e"), expect: Some("#1020, rel#4; #1020, rel#5; #1021, rel#6; #1022, rel#7; #1023, rel#8; #1024, rel#9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.2", name: "Parenthesized value expressions", probe: Some("RETURN (1 + 2) * 3 AS v"), expect: Some("9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.3 SESSION_USER", name: "`SESSION_USER`", probe: Some("RETURN SESSION_USER AS u"), expect: Some("\"anonymous\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Extension("a variable may also be named `session_user`, which GQL reserves; `SESSION_USER` then means it.") },
    Feature { id: "20.4", name: "Dynamic parameters `$name`", probe: Some("RETURN $x AS v"), expect: Some("1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.7 searched", name: "Searched `CASE`", probe: Some("RETURN CASE WHEN 1 > 2 THEN 'a' WHEN 1 < 2 THEN 'b' ELSE 'c' END AS v"), expect: Some("\"b\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.7 simple", name: "Simple `CASE`", probe: Some("RETURN CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'b' END AS v"), expect: Some("\"b\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.7 operands", name: "Simple `CASE` with operand lists and predicate operands", probe: Some("RETURN CASE 3 WHEN 1, 2 THEN 'a' WHEN > 2 THEN 'b' END AS v"), expect: Some("\"b\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.7 NULLIF", name: "`NULLIF`", probe: Some("RETURN nullif(1, 1) AS v"), expect: Some("null"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.7 COALESCE", name: "`COALESCE`", probe: Some("RETURN coalesce(null, 1) AS v"), expect: Some("1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.9 COUNT(*)", name: "`COUNT(*)`", probe: Some("MATCH (i:Item) RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.9", name: "`COUNT` `SUM` `AVG` `MIN` `MAX`", probe: Some("MATCH (i:Item) RETURN count(i.name) AS a, sum(i.score) AS b, avg(i.score) AS c, min(i.score) AS d, max(i.score) AS e"), expect: Some("5, 11, 2.2f, 0, 5"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Deviation("`SUM` over no values is `0`, not null; and without `GROUP BY`, items that do not aggregate group the result, as in Cypher.") },
    Feature { id: "20.9 DISTINCT", name: "`DISTINCT` and `ALL` in aggregates", probe: Some("MATCH (i:Item) RETURN count(DISTINCT i.kind) AS a, count(ALL i.kind) AS b"), expect: Some("2, 6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.11", name: "Property references", probe: Some("MATCH (n:Hub) RETURN n.name AS v"), expect: Some("\"hub\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.12", name: "Binding variable references", probe: Some("MATCH (n:Hub) RETURN n"), expect: Some("#1010"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.20", name: "`AND` `OR` `NOT`", probe: Some("RETURN true AND false AS a, true OR false AS b, NOT true AS c, (true OR false) AND true AS d"), expect: Some("false, true, false, true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.20 IS", name: "`IS [NOT] TRUE`, `FALSE`, `UNKNOWN`", probe: Some("RETURN true IS TRUE AS a, false IS NOT FALSE AS b, null IS UNKNOWN AS c"), expect: Some("true, false, true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.21", name: "`+ - * /` on INTEGER and FLOAT", probe: Some("RETURN 7 / 2 + 1.5E0 * 2 - 1 AS v"), expect: Some("5.0f"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.21 sign", name: "Unary `+` and `-`", probe: Some("RETURN -(1) AS a, +2 AS b"), expect: Some("-1, 2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.22", name: "`CHAR_LENGTH` / `CHARACTER_LENGTH`", probe: Some("RETURN char_length('abc') AS v, character_length('') AS w"), expect: Some("3, 0"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.23", name: "`||` on character strings", probe: Some("RETURN 'a' || 'b' AS v"), expect: Some("\"ab\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.23 bytes", name: "Byte string `||`, `LEFT` and `RIGHT`", probe: None, expect: None, levels: Some(["—", "—", "—"]), diagnostics: Diagnostics::Judged("—"), claimed: false, note: Note::Reason("Not applicable: nothing mandatory makes a byte string. Byte string literals and types need GV35, and the engine stores no byte string property values.") },
    Feature { id: "20.24 fold", name: "`UPPER` / `LOWER`", probe: Some("RETURN upper('a') AS a, lower('B') AS b"), expect: Some("\"A\", \"b\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.24 LEFT", name: "`LEFT` / `RIGHT`", probe: Some("RETURN left('abc', 1) AS a, right('abc', 1) AS b"), expect: Some("\"a\", \"c\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.24 TRIM", name: "`TRIM(source)`", probe: Some("RETURN trim(' a ') AS v"), expect: Some("\"a\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "20.24 NORMALIZE", name: "`NORMALIZE`", probe: Some("RETURN normalize('a') AS a, normalize('a', NFD) AS b"), expect: Some("\"a\", \"a\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "21.2 strings", name: "Character string literals, single- and double-quoted", probe: Some("RETURN 'a' AS a, \"b\" AS b"), expect: Some("\"a\", \"b\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "21.2 doubled", name: "A doubled quote inside a string literal", probe: Some("RETURN 'it''s' AS a, \"a\"\"b\" AS b"), expect: Some("\"it's\", \"a\\\"b\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "21.2 escapes", name: "Escapes in string literals", probe: Some("RETURN 'a\\tb' AS a, 'a\\u0041' AS b"), expect: Some("\"a\\tb\", \"aA\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "21.2 integers", name: "Integer literals, with `_` digit separators", probe: Some("RETURN 1 AS a, 1_000 AS b"), expect: Some("1, 1000"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "21.2 floats", name: "Float literals in scientific notation", probe: Some("RETURN 1.5E3 AS a, 2E-1 AS b, 1e+2 AS c"), expect: Some("1500.0f, 0.2f, 100.0f"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "21.2 booleans", name: "`TRUE` `FALSE` `NULL`", probe: Some("RETURN TRUE AS a, FALSE AS b, NULL AS c"), expect: Some("true, false, null"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "21.2 UNKNOWN", name: "`UNKNOWN` literal", probe: Some("RETURN UNKNOWN AS v"), expect: Some("null"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Extension("a variable may also be named `unknown`, which GQL reserves; `UNKNOWN` then means it.") },
    Feature { id: "21.3", name: "Regular identifiers; keywords in any case", probe: Some("match (n:Hub) return n.name as name"), expect: Some("\"hub\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "21.3 accent", name: "Accent-quoted (backtick) identifiers", probe: Some("MATCH (`odd name`:Hub) RETURN `odd name`.name AS `n``1`"), expect: Some("\"hub\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "21.3 quoted", name: "Double-quoted identifiers", probe: Some("MATCH (n:\"Hub\") RETURN n.name AS n"), expect: Some("\"hub\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "21.3 comments", name: "Bracketed comments `/* */`", probe: Some("MATCH (n:Hub) /* a comment */ RETURN n.name AS n"), expect: Some("\"hub\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "21.3 reserved", name: "Reserved words not usable as names", probe: None, expect: None, levels: Some(["✅", "✅", "✅"]), diagnostics: Diagnostics::Judged("—"), claimed: false, note: Note::Extension("names GQL reserves, such as `count`, `type` or `value`, are also accepted, with Cypher's meaning.") },
    Feature { id: "4.9", name: "GQL-status objects, GQLSTATUS (§4.9, §23)", probe: None, expect: None, levels: Some(["—", "—", "—"]), diagnostics: Diagnostics::Judged("✅"), claimed: false, note: Note::Reason("Every outcome and error has its GQL-status object (`ExecutionOutcome`, `EngineError::status_object`), and the server sends them: Bolt FAILURE gets `gql_status`, `description` and `diagnostic_record`, Bolt SUCCESS `statuses`, and HTTP bodies the same fields.") },
    Feature { id: "G002", name: "Different-edges match mode", probe: Some("MATCH DIFFERENT EDGES (a)-[]->(b) RETURN count(*) AS c"), expect: Some("9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G003", name: "Explicit REPEATABLE ELEMENTS keyword", probe: Some("MATCH REPEATABLE ELEMENTS (a)-[]->(b) RETURN count(*) AS c"), expect: Some("9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G004", name: "Path variables", probe: Some("MATCH p = (a)-[]->(b) RETURN count(*) AS c"), expect: Some("9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G005", name: "Path search prefix in a path pattern", probe: Some("MATCH ANY (a)-[]->(b) RETURN count(*) AS c"), expect: Some("9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G006", name: "Graph pattern KEEP clause: path mode prefix", probe: Some("MATCH (a)-[]->(b) KEEP TRAIL RETURN count(*) AS c"), expect: Some("9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G007", name: "Graph pattern KEEP clause: path search prefix", probe: Some("MATCH (a)-[]->(b) KEEP ANY SHORTEST RETURN count(*) AS c"), expect: Some("9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G010", name: "Explicit WALK keyword", probe: Some("MATCH WALK (a)-[]->(b) RETURN count(*) AS c"), expect: Some("9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G011", name: "Advanced path modes: TRAIL", probe: Some("MATCH TRAIL (a)-[]->(b) RETURN count(*) AS c"), expect: Some("9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G012", name: "Advanced path modes: SIMPLE", probe: Some("MATCH SIMPLE (a)-[]->(b) RETURN count(*) AS c"), expect: Some("9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G013", name: "Advanced path modes: ACYCLIC", probe: Some("MATCH ACYCLIC (a)-[]->(b) RETURN count(*) AS c"), expect: Some("8"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G014", name: "Explicit PATH/PATHS keywords", probe: Some("MATCH TRAIL PATH (a)-[]->(b) RETURN count(*) AS c"), expect: Some("9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G015", name: "All path search: explicit ALL keyword", probe: Some("MATCH ALL (a)-[]->(b) RETURN count(*) AS c"), expect: Some("9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G016", name: "Any path search", probe: Some("MATCH ANY 2 (a)-[]->(b) RETURN count(*) AS c"), expect: Some("9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G017", name: "All shortest path search", probe: Some("MATCH ALL SHORTEST (a:Node {name: 'a'})-[]->{1,3}(b:Node {name: 'd'}) RETURN count(*) AS c"), expect: Some("2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G018", name: "Any shortest path search", probe: Some("MATCH ANY SHORTEST (a:Node {name: 'a'})-[]->{1,3}(b:Node {name: 'd'}) RETURN count(*) AS c"), expect: Some("1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G019", name: "Counted shortest path search", probe: Some("MATCH SHORTEST 2 (a:Node {name: 'a'})-[]->{1,3}(b:Node {name: 'd'}) RETURN count(*) AS c"), expect: Some("2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G020", name: "Counted shortest group search", probe: Some("MATCH SHORTEST 2 GROUPS (a:Node {name: 'a'})-[]->{1,3}(b:Node {name: 'd'}) RETURN count(*) AS c"), expect: Some("2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G030", name: "Path multiset alternation", probe: Some("MATCH (a)-[:R]->(b) |+| (a)<-[:R]-(b) RETURN count(*) AS c"), expect: Some("12"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G031", name: "Path multiset alternation: variable length path operands", probe: Some("MATCH (a)-[:R]->{1,2}(b) |+| (a)<-[:R]-(b) RETURN count(*) AS c"), expect: Some("18"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G032", name: "Path pattern union", probe: Some("MATCH ((a)-[:R]->(b) | (a)<-[:R]-(b)) RETURN count(*) AS c"), expect: Some("11"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G033", name: "Path pattern union: variable length path operands", probe: Some("MATCH ((a)-[:R]->{1,2}(b) | (a)<-[:R]-(b)) RETURN count(*) AS c"), expect: Some("17"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G035", name: "Quantified paths", probe: Some("MATCH ((a)-[:R]->(b)){1,2} RETURN count(*) AS c"), expect: Some("12"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G036", name: "Quantified edges", probe: Some("MATCH (a)-[:R]->{1,2}(b) RETURN count(*) AS c"), expect: Some("12"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G037", name: "Questioned paths", probe: Some("MATCH (h:Hub)-[e:HAS]->?(i) RETURN count(*) AS c"), expect: Some("4"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Limit("a `?` inside a path pattern union operand, after a quantifier, or beyond three in one path term is refused (`0A000`); see `deviations.md` L22.") },
    Feature { id: "G038", name: "Parenthesized path pattern expression", probe: Some("MATCH ((a)-[:R]->(b)) RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G039", name: "Simplified path pattern expression: full defaulting", probe: Some("MATCH (a)<-/R/->(b) RETURN count(*) AS c"), expect: Some("11"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G041", name: "Non-local element pattern predicates", probe: Some("MATCH (h:Hub)-[e:HAS]->(i WHERE i.score > e.weight) RETURN count(*) AS c"), expect: Some("1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G043", name: "Complete full edge patterns", probe: Some("MATCH (a)<~[e:R]~(b) RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G044", name: "Basic abbreviated edge patterns", probe: Some("MATCH (a:Node {name: 'a'})->(b) RETURN count(*) AS c"), expect: Some("2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Extension("Cypher's `-->`, `<--` and `--` are also accepted; GQL would read `--` as the start of a comment (GB02, not claimed).") },
    Feature { id: "G045", name: "Complete abbreviated edge patterns", probe: Some("MATCH (a)<~(b) RETURN count(*) AS c"), expect: Some("9"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G046", name: "Relaxed topological consistency: adjacent vertex patterns", probe: Some("MATCH (a:Hub)(b) RETURN count(*) AS c"), expect: Some("1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G047", name: "Relaxed topological consistency: concise edge patterns", probe: Some("MATCH (a)-[:R]->-[:R]->(b) RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G048", name: "Parenthesized path pattern: subpath variable declaration", probe: Some("MATCH (p = (a)-[:R]->(b)) RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G049", name: "Parenthesized path pattern: path mode prefix", probe: Some("MATCH (TRAIL (a)-[:R]->(b)) RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G050", name: "Parenthesized path pattern: WHERE clause", probe: Some("MATCH ((a)-[e:HAS]->(b) WHERE e.weight > 1) RETURN count(*) AS c"), expect: Some("2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G051", name: "Parenthesized path pattern: non-local predicates", probe: Some("MATCH (x:Hub), ((a)-[e:HAS]->(b) WHERE a = x) RETURN count(*) AS c"), expect: Some("3"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G060", name: "Bounded graph pattern quantifiers", probe: Some("MATCH (a)-[:R]->{1,3}(b) RETURN count(*) AS c"), expect: Some("20"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G061", name: "Unbounded graph pattern quantifiers", probe: Some("MATCH (a:Node {name: 'a'})-[:R]->+(b) RETURN count(*) AS c"), expect: Some("10"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G074", name: "Label expression: wildcard label", probe: Some("MATCH (n:%) RETURN count(*) AS c"), expect: Some("12"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G080", name: "Simplified path pattern expression: basic defaulting", probe: Some("MATCH (a)-/R/->(b) RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G081", name: "Simplified path pattern expression: full overrides", probe: Some("MATCH (a)-/<R>/-(b) RETURN count(*) AS c"), expect: Some("11"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G082", name: "Simplified path pattern expression: basic overrides", probe: Some("MATCH (a)-/R>/-(b) RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G100", name: "ELEMENT_ID function", probe: Some("MATCH (n:Hub) RETURN element_id(n) = element_id(n) AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G110", name: "IS DIRECTED predicate", probe: Some("MATCH ()-[e:R]->() RETURN e IS DIRECTED AS d"), expect: Some("true; true; true; true; true; true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G111", name: "IS LABELED predicate", probe: Some("MATCH (n) WHERE n IS LABELED Hub RETURN count(*) AS c"), expect: Some("1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G112", name: "IS SOURCE and IS DESTINATION predicate", probe: Some("MATCH (a)-[e:R]->(b) WHERE a IS SOURCE OF e RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G113", name: "ALL_DIFFERENT predicate", probe: Some("MATCH (a)-[:R]->(b) WHERE ALL_DIFFERENT(a, b) RETURN count(*) AS c"), expect: Some("5"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G114", name: "SAME predicate", probe: Some("MATCH (a)-[:R]->(b) WHERE SAME(a, b) RETURN count(*) AS c"), expect: Some("1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "G115", name: "PROPERTY_EXISTS predicate", probe: Some("MATCH (i:Item) WHERE PROPERTY_EXISTS(i, name) RETURN count(*) AS c"), expect: Some("5"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GA01", name: "IEEE 754 floating point operations", probe: None, expect: None, levels: Some(["—", "—", "—"]), diagnostics: Diagnostics::Judged("—"), claimed: false, note: Note::Reason("Not claimed: an infinite or NaN result raises `22003`, a zero divisor `22012`.") },
    Feature { id: "GA03", name: "Explicit ordering of nulls", probe: Some("MATCH (i:Item) RETURN i.score AS s ORDER BY s NULLS FIRST"), expect: Some("null; 0; 1; 2; 3; 5"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GA04", name: "Universal comparison", probe: None, expect: None, levels: Some(["—", "—", "➖"]), diagnostics: Diagnostics::Judged("—"), claimed: false, note: Note::Reason("Not claimed. Values of different types sort in Cypher's order (US007) and compare to null, which partly stands in for it.") },
    Feature { id: "GA05", name: "Cast specification", probe: Some("RETURN CAST(1 AS STRING) AS v"), expect: Some("\"1\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GA06", name: "Value type predicate", probe: Some("RETURN 1 IS :: INTEGER AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GA07", name: "Ordering by discarded binding variables", probe: Some("MATCH (n:Hub) RETURN n.name AS v ORDER BY n"), expect: Some("\"hub\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GA08", name: "GQL-status objects with diagnostic records", probe: None, expect: None, levels: Some(["—", "—", "—"]), diagnostics: Diagnostics::Judged("✅"), claimed: false, note: Note::Reason("Supported in the engine API: every exception's status object carries its diagnostic record (§23.2).") },
    Feature { id: "GA09", name: "Comparison of paths", probe: Some("MATCH REPEATABLE ELEMENTS p = (a:Node {name: 'b'})-[:R]->(b), q = (a)-[:R]->(b) RETURN p = q AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GB01", name: "Long identifiers", probe: Some("RETURN 1 AS a_name_that_is_longer_than_one_hundred_and_twenty_seven_characters_which_gql_allows_only_with_the_long_identifiers_feature_enabled_here"), expect: Some("1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GB02", name: "Double minus sign comments", probe: Some("RETURN 1 AS v -- a comment"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GB03", name: "Double solidus comments", probe: Some("RETURN 1 AS v // a comment"), expect: Some("1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GC00", name: "Automatic graph population (Cor 1)", probe: None, expect: None, levels: Some(["—", "—", "—"]), diagnostics: Diagnostics::Judged("—"), claimed: true, note: Note::Reason("Claimed: the engine reads a graph that already exists, which is the claim §24.2 needs after Cor 1.") },
    Feature { id: "GC01", name: "Graph schema management", probe: Some("CREATE SCHEMA /s"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GC02", name: "Graph schema management: IF [ NOT ] EXISTS", probe: Some("CREATE SCHEMA IF NOT EXISTS /s"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GC03", name: "Graph type: IF [ NOT ] EXISTS", probe: Some("CREATE GRAPH TYPE IF NOT EXISTS t {(:A)}"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GC04", name: "Graph management", probe: Some("CREATE GRAPH g ANY"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GC05", name: "Graph management: IF [ NOT ] EXISTS", probe: Some("CREATE GRAPH IF NOT EXISTS g ANY"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GD01", name: "Updatable graphs", probe: Some("INSERT (:Item {name: 'new'})"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GD02", name: "Graph label set changes", probe: Some("MATCH (n:Hub) SET n:Extra"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GD03", name: "DELETE statement: subquery support", probe: Some("MATCH (n:Hub) DELETE CALL { RETURN n }"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GD04", name: "DELETE statement: simple expression support", probe: Some("MATCH (n:Hub) DETACH DELETE n"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GE01", name: "Graph reference value expressions", probe: Some("RETURN GRAPH CURRENT_GRAPH AS g"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GE02", name: "Binding table reference value expressions", probe: Some("RETURN TABLE t AS g"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GE03", name: "Let-binding of variables in expressions", probe: Some("RETURN LET x = 1 IN x + 1 END AS v"), expect: Some("2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Limit("an aggregate inside `LET` is refused (`0A000`); see `deviations.md` L24.") },
    Feature { id: "GE04", name: "Graph parameters", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GE05", name: "Binding table parameters", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GE06", name: "Path value construction", probe: Some("MATCH (a:Node {name: 'a'})-[e:R]->(b:Node {name: 'b'}) RETURN PATH[a, e, b] AS p"), expect: Some("<#1020, rel#4, #1021>"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Limit("`PATH[...]` reads only the nodes and edges the record binds; see `deviations.md` L24.") },
    Feature { id: "GE07", name: "Boolean XOR", probe: Some("RETURN true XOR false AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GE08", name: "Reference parameters", probe: Some("MATCH (n:Hub) RETURN $$p AS v"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GE09", name: "Horizontal aggregation", probe: Some("MATCH (a:Node {name: 'a'})-[e:R]->{2}(b) WHERE count(e) = 2 RETURN count(*) AS c"), expect: Some("2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Limit("in a graph pattern's `WHERE` and in `FILTER` only, and with no `01G11` warning; see `deviations.md` L23.") },
    Feature { id: "GF01", name: "Enhanced numeric functions", probe: Some("RETURN abs(-1) AS a, mod(5, 2) AS b, floor(1.5E0) AS c, ceil(1.5E0) AS d, sqrt(4) AS e"), expect: Some("1, 1, 1.0f, 2.0f, 2.0f"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GF02", name: "Trigonometric functions", probe: Some("RETURN sin(0) AS a, cos(0) AS b, tan(0) AS c, asin(0) AS d, degrees(0) AS e"), expect: Some("0.0f, 1.0f, 0.0f, 0.0f, 0.0f"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GF03", name: "Logarithmic functions", probe: Some("RETURN log(2, 8) AS a, log10(10) AS b, ln(1) AS c, exp(0) AS d, power(2, 3) AS e"), expect: Some("3.0f, 1.0f, 0.0f, 1.0f, 8.0f"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GF04", name: "Enhanced path functions", probe: Some("MATCH p = (a)-[:R]->(b) RETURN path_length(p) AS n"), expect: Some("1; 1; 1; 1; 1; 1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GF05", name: "Multi-character TRIM function", probe: Some("RETURN ltrim('xxa', 'x') AS a, rtrim('axx', 'x') AS b, btrim('xax', 'x') AS c"), expect: Some("\"a\", \"a\", \"a\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Deviation("a character and source that cannot be compared at runtime raise `22G03` instead of `22G04`.") },
    Feature { id: "GF06", name: "Explicit TRIM function", probe: Some("RETURN trim(LEADING 'x' FROM 'xa') AS v"), expect: Some("\"a\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Deviation("a character and source that cannot be compared at runtime raise `22G03` instead of `22G04`.") },
    Feature { id: "GF07", name: "Byte string TRIM function", probe: Some("RETURN trim(X'00' FROM X'0011') AS v"), expect: Some("X'11'"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GF10", name: "Advanced aggregate functions: general set functions", probe: Some("MATCH (i:Item) RETURN size(collect_list(i.score)) AS a, stddev_samp(i.score) AS b"), expect: Some("5, 1.9235384061671346f"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GF11", name: "Advanced aggregate functions: binary set functions", probe: Some("MATCH (i:Item) RETURN percentile_cont(i.score, 0.5) AS v"), expect: Some("2f"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GF12", name: "CARDINALITY function", probe: Some("RETURN cardinality([1, 2]) AS v"), expect: Some("2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GF13", name: "SIZE function", probe: Some("RETURN size([1, 2]) AS v"), expect: Some("2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GF20", name: "Aggregate functions in sort keys", probe: Some("MATCH (i:Item) RETURN i.kind AS k, count(*) AS c GROUP BY k ORDER BY count(*)"), expect: Some("\"veg\", 2; \"fruit\", 4"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GG01", name: "Graph with an open graph type", probe: None, expect: None, levels: Some(["—", "—", "—"]), diagnostics: Diagnostics::Judged("—"), claimed: false, note: Note::Reason("No graph DDL; the graph the engine reads has an open type.") },
    Feature { id: "GG02", name: "Graph with a closed graph type", probe: Some("CREATE GRAPH TYPE t {(:A)}"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GG03", name: "Graph type inline specification", probe: Some("CREATE GRAPH g {(:A)}"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GG04", name: "Graph type like a graph", probe: Some("CREATE GRAPH g LIKE CURRENT_GRAPH"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GG05", name: "Graph from a graph source", probe: Some("CREATE GRAPH g ANY AS COPY OF CURRENT_GRAPH"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GG20", name: "Explicit element type names", probe: Some("CREATE GRAPH TYPE t {NODE TYPE Person (:Person)}"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GG21", name: "Explicit element type key label sets", probe: Some("CREATE GRAPH TYPE t {(:Person => :Person)}"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GG22", name: "Element type key label set inference", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GG23", name: "Optional element type key label sets", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GG24", name: "Relaxed structural consistency", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GG25", name: "Relaxed key label set uniqueness for edge types", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GG26", name: "Relaxed property value type consistency", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GH01", name: "External object references", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GH02", name: "Undirected edge patterns", probe: Some("INSERT (:Node)~[:R]~(:Node)"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GL01", name: "Hexadecimal literals", probe: Some("RETURN 0x1F AS v"), expect: Some("31"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GL02", name: "Octal literals", probe: Some("RETURN 0o17 AS v"), expect: Some("15"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GL03", name: "Binary literals", probe: Some("RETURN 0b101 AS v"), expect: Some("5"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GL04", name: "Exact number in common notation without suffix", probe: Some("RETURN 1.5 AS v"), expect: Some("1.5"), levels: Some(["✅", "✅", "➖"]), diagnostics: Diagnostics::Judged("—"), claimed: false, note: Note::Reason("Read as an approximate FLOAT, as Cypher does (X17); `1.5M` is the exact decimal (GL05).") },
    Feature { id: "GL05", name: "Exact number in common notation or as decimal integer with suffix", probe: Some("RETURN 1.5M AS v"), expect: Some("1.5M"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GL06", name: "Exact number in scientific notation with suffix", probe: Some("RETURN 1.5E3M AS v"), expect: Some("1500M"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GL07", name: "Approximate number in common notation or as decimal integer with suffix", probe: Some("RETURN 1.5F AS v"), expect: Some("1.5f"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GL08", name: "Approximate number in scientific notation with suffix", probe: Some("RETURN 1.5E3F AS v"), expect: Some("1500.0f"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GL09", name: "Optional float number suffix", probe: Some("RETURN 1.5E3f AS v"), expect: Some("1500.0f"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GL10", name: "Optional double number suffix", probe: Some("RETURN 1.5E3D AS v"), expect: Some("1500.0f"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GL11", name: "Opt-out character escaping", probe: Some("RETURN @'a\\b' AS v"), expect: Some("\"a\\\\b\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GL12", name: "SQL datetime and interval formats", probe: Some("RETURN TIMESTAMP '2026-01-01 12:00:00' AS v"), expect: Some("DATETIME '2026-01-01T12:00:00.000000000'"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP01", name: "Inline procedure", probe: Some("MATCH (n:Hub) CALL { MATCH (m:Node) RETURN count(*) AS c } RETURN c"), expect: Some("5"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Limit("a call whose query ends in `FINISH` or returns `*`, and `IN TRANSACTIONS`, are refused (`0A000`); see `deviations.md` L25.") },
    Feature { id: "GP02", name: "Inline procedure with implicit nested variable scope", probe: Some("MATCH (n:Hub) CALL { RETURN n.name AS x } RETURN x"), expect: Some("\"hub\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Limit("as GQL says, a call with no scope clause imports every name in scope, where Cypher 25 imports none; see `deviations.md`.") },
    Feature { id: "GP03", name: "Inline procedure with explicit nested variable scope", probe: Some("MATCH (n:Hub) CALL (n) { RETURN n.name AS x } RETURN x"), expect: Some("\"hub\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP04", name: "Named procedure calls", probe: Some("CALL db.labels() YIELD label RETURN label"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP05", name: "Procedure-local value variable definitions", probe: Some("VALUE x = 1 RETURN x"), expect: Some("1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Limit("a declared type, `VALUE x :: INT = 1`, is refused (`0A000`); see `deviations.md` L26.") },
    Feature { id: "GP06", name: "Procedure-local value variable definitions: value variables based on simple expressions", probe: Some("VALUE x = 2 VALUE y = x + 1 RETURN y"), expect: Some("3"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP07", name: "Procedure-local value variable definitions: value variable based on subqueries", probe: Some("VALUE n = VALUE { MATCH (m:Node) RETURN count(*) AS c } RETURN n"), expect: Some("5"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP08", name: "Procedure-local binding table variable definitions", probe: Some("TABLE t = { MATCH (n) RETURN n } RETURN 1 AS v"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP09", name: "Procedure-local binding table variable definitions: binding table variables based on simple expressions or references", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP10", name: "Procedure-local binding table variable definitions: binding table variables based on subqueries", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP11", name: "Procedure-local graph variable definitions", probe: Some("GRAPH g = CURRENT_GRAPH RETURN 1 AS v"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP12", name: "Procedure-local graph variable definitions: graph variables based on simple expressions or references", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP13", name: "Procedure-local graph variable definitions: graph variables based on subqueries", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP14", name: "Binding tables as procedure arguments", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP15", name: "Graphs as procedure arguments", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP16", name: "AT schema clause", probe: Some("AT /s MATCH (n) RETURN n"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP17", name: "Binding variable definition block", probe: Some("VALUE k = 'fruit' MATCH (i:Item) WHERE i.kind = k RETURN count(*) AS c"), expect: Some("4"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GP18", name: "Catalog and data statement mixing", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ01", name: "USE graph clause", probe: Some("USE g MATCH (n) RETURN n"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ02", name: "Composite query: OTHERWISE", probe: Some("MATCH (n:Missing) RETURN n.name AS v OTHERWISE MATCH (n:Hub) RETURN n.name AS v"), expect: Some("\"hub\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ03", name: "Composite query: UNION", probe: Some("MATCH (n:Hub) RETURN n.name AS v UNION ALL MATCH (n:Node {name: 'a'}) RETURN n.name AS v"), expect: Some("\"a\"; \"hub\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Deviation("under `UNION DISTINCT`, values that cannot be compared are distinct instead of raising `22G04`.") },
    Feature { id: "GQ04", name: "Composite query: EXCEPT DISTINCT", probe: Some("MATCH (n:Node) RETURN n.name AS v EXCEPT MATCH (n:Node {name: 'a'}) RETURN n.name AS v"), expect: Some("\"b\"; \"c\"; \"d\"; \"loop\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Deviation("under `EXCEPT`, values that cannot be compared are distinct instead of raising `22G04`.") },
    Feature { id: "GQ05", name: "Composite query: EXCEPT ALL", probe: Some("MATCH (n:Node) RETURN n.name AS v EXCEPT ALL MATCH (n:Node {name: 'a'}) RETURN n.name AS v"), expect: Some("\"b\"; \"c\"; \"d\"; \"loop\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Deviation("under `EXCEPT ALL`, values that cannot be compared are distinct instead of raising `22G04`.") },
    Feature { id: "GQ06", name: "Composite query: INTERSECT DISTINCT", probe: Some("MATCH (n:Node) RETURN n.name AS v INTERSECT MATCH (n:Node {name: 'a'}) RETURN n.name AS v"), expect: Some("\"a\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Deviation("under `INTERSECT`, values that cannot be compared are distinct instead of raising `22G04`.") },
    Feature { id: "GQ07", name: "Composite query: INTERSECT ALL", probe: Some("MATCH (n:Node) RETURN n.name AS v INTERSECT ALL MATCH (n:Node {name: 'a'}) RETURN n.name AS v"), expect: Some("\"a\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Deviation("under `INTERSECT ALL`, values that cannot be compared are distinct instead of raising `22G04`.") },
    Feature { id: "GQ08", name: "FILTER statement", probe: Some("MATCH (i:Item) FILTER i.score > 2 RETURN count(*) AS c"), expect: Some("2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ09", name: "LET statement", probe: Some("MATCH (i:Item) LET s = i.score * 2 RETURN s ORDER BY s"), expect: Some("0; 2; 4; 6; 10; null"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ10", name: "FOR statement: list value support", probe: Some("FOR x IN [1, 2] RETURN x"), expect: Some("1; 2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ11", name: "FOR statement: WITH ORDINALITY", probe: Some("FOR x IN [1, 2] WITH ORDINALITY i RETURN x, i"), expect: Some("1, 1; 2, 2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ12", name: "ORDER BY and page statement: OFFSET clause", probe: Some("MATCH (i:Item) RETURN i.name AS n ORDER BY n OFFSET 1"), expect: Some("\"apple\"; \"apple\"; \"banana\"; \"carrot\"; null"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ13", name: "ORDER BY and page statement: LIMIT clause", probe: Some("MATCH (i:Item) RETURN i.name AS n ORDER BY n LIMIT 1"), expect: Some("\"Durian\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ14", name: "Complex expressions in sort keys", probe: Some("MATCH (i:Item) RETURN i.name AS n ORDER BY i.score * -1"), expect: Some("null; \"apple\"; \"carrot\"; \"banana\"; \"apple\"; \"Durian\""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ15", name: "GROUP BY clause", probe: Some("MATCH (i:Item) RETURN i.kind AS k, count(*) AS c GROUP BY k"), expect: Some("\"fruit\", 4; \"veg\", 2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ16", name: "Pre-projection aliases in sort keys", probe: Some("MATCH (i:Item) RETURN i.score AS s ORDER BY i.score"), expect: Some("0; 1; 2; 3; 5; null"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ17", name: "Element-wise group variable operations", probe: Some("MATCH (h:Hub)-[e:HAS]->{1}(i) WHERE sum(e.weight) > 1 RETURN count(*) AS c"), expect: Some("2"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ18", name: "Scalar subqueries", probe: Some("RETURN VALUE { MATCH (i:Item) RETURN count(*) AS c } AS v"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ19", name: "Graph pattern YIELD clause", probe: Some("MATCH (a)-[:R]->(b) YIELD a RETURN count(*) AS c"), expect: Some("6"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ20", name: "Advanced linear composition with NEXT", probe: Some("MATCH (n:Hub) RETURN n NEXT RETURN 1 AS v"), expect: Some("1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ21", name: "OPTIONAL: Multiple MATCH statements", probe: Some("OPTIONAL { MATCH (n:Hub) MATCH (n)-[:R]->(m) } RETURN count(*) AS c"), expect: Some("1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ22", name: "EXISTS predicate: multiple MATCH statements", probe: Some("MATCH (n:Node) WHERE EXISTS { MATCH (n)-[:R]->(m) MATCH (m)-[:R]->(o) } RETURN count(*) AS c"), expect: Some("5"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ23", name: "FOR statement: binding table support", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ24", name: "FOR statement: WITH OFFSET", probe: Some("FOR x IN [1, 2] WITH OFFSET i RETURN x, i"), expect: Some("1, 0; 2, 1"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GQ27", name: "FINISH statement (Cor 1)", probe: Some("MATCH (n:Hub) FINISH"), expect: Some(""), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS01", name: "SESSION SET command: session-local graph parameters", probe: Some("SESSION SET GRAPH $g = CURRENT_GRAPH"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS02", name: "SESSION SET command: session-local binding table parameters", probe: Some("SESSION SET TABLE $t = { MATCH (n) RETURN n }"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS03", name: "SESSION SET command: session-local value parameters", probe: Some("SESSION SET VALUE $v = 1"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS04", name: "SESSION RESET command: reset all characteristics", probe: Some("SESSION RESET"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS05", name: "SESSION RESET command: reset session schema", probe: Some("SESSION RESET SCHEMA"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS06", name: "SESSION RESET command: reset session graph", probe: Some("SESSION RESET GRAPH"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS07", name: "SESSION RESET command: reset time zone displacement", probe: Some("SESSION RESET TIME ZONE"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS08", name: "SESSION RESET command: reset all session parameters", probe: Some("SESSION RESET ALL PARAMETERS"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS09", name: "SESSION SET command: set session schema (Cor 1)", probe: Some("SESSION SET SCHEMA /s"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS10", name: "SESSION SET command: session-local binding table parameters based on subqueries", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS11", name: "SESSION SET command: session-local value parameters based on subqueries", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS12", name: "SESSION SET command: session-local graph parameters based on simple expressions or references", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS13", name: "SESSION SET command: session-local binding table parameters based on simple expressions or references", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS14", name: "SESSION SET command: session-local value parameters based on simple expressions", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS15", name: "SESSION SET command: set time zone displacement", probe: Some("SESSION SET TIME ZONE '+01:00'"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS16", name: "SESSION RESET command: reset individual session parameters", probe: Some("SESSION RESET PARAMETER $v"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS17", name: "SESSION SET command: set session graph (Cor 1)", probe: Some("SESSION SET GRAPH CURRENT_GRAPH"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GS18", name: "SESSION CLOSE command (Cor 1)", probe: Some("SESSION CLOSE"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GT01", name: "Explicit transaction commands", probe: Some("START TRANSACTION"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GT02", name: "Specified transaction characteristics", probe: Some("START TRANSACTION READ ONLY"), expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GT03", name: "Use of multiple graphs in a transaction", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV01", name: "8 bit unsigned integer numbers", probe: Some("RETURN 1 IS :: UINT8 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV02", name: "8 bit signed integer numbers", probe: Some("RETURN 1 IS :: INT8 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV03", name: "16 bit unsigned integer numbers", probe: Some("RETURN 1 IS :: UINT16 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV04", name: "16 bit signed integer numbers", probe: Some("RETURN 1 IS :: INT16 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV05", name: "Small unsigned integer numbers", probe: Some("RETURN 1 IS :: USMALLINT AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV06", name: "32 bit unsigned integer numbers", probe: Some("RETURN 1 IS :: UINT32 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV07", name: "32 bit signed integer numbers", probe: Some("RETURN 1 IS :: INT32 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV08", name: "Regular unsigned integer numbers", probe: Some("RETURN 1 IS :: UINT AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV09", name: "Specified integer number precision", probe: Some("RETURN 1 IS :: INT(10) AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV10", name: "Big unsigned integer numbers", probe: Some("RETURN 1 IS :: UBIGINT AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV11", name: "64 bit unsigned integer numbers", probe: Some("RETURN 1 IS :: UINT64 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV12", name: "64 bit signed integer numbers", probe: Some("RETURN 1 IS :: INT64 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV13", name: "128 bit unsigned integer numbers", probe: Some("RETURN 1 IS :: UINT128 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV14", name: "128 bit signed integer numbers", probe: Some("RETURN 1 IS :: INT128 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV15", name: "256 bit unsigned integer numbers", probe: Some("RETURN 1 IS :: UINT256 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV16", name: "256 bit signed integer numbers", probe: Some("RETURN 1 IS :: INT256 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV17", name: "Decimal numbers", probe: Some("RETURN 1 IS :: DECIMAL AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV18", name: "Small signed integer numbers", probe: Some("RETURN 1 IS :: SMALLINT AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV19", name: "Big signed integer numbers", probe: Some("RETURN 1 IS :: BIGINT AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV20", name: "16 bit floating point numbers", probe: Some("RETURN 1.0 IS :: FLOAT16 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV21", name: "32 bit floating point numbers", probe: Some("RETURN 1.0 IS :: FLOAT32 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV22", name: "Specified floating point number precision", probe: Some("RETURN 1.0 IS :: FLOAT(10) AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV23", name: "Floating point type name synonyms", probe: Some("RETURN 1.0 IS :: DOUBLE AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV24", name: "64 bit floating point numbers", probe: Some("RETURN 1.0 IS :: FLOAT64 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV25", name: "128 bit floating point numbers", probe: Some("RETURN 1.0 IS :: FLOAT128 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV26", name: "256 bit floating point numbers", probe: Some("RETURN 1.0 IS :: FLOAT256 AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV30", name: "Specified character string minimum length", probe: Some("RETURN 'a' IS :: STRING(1, 10) AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV31", name: "Specified character string maximum length", probe: Some("RETURN 'a' IS :: STRING(10) AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV32", name: "Specified character string fixed length", probe: Some("RETURN 'a' IS :: CHAR(1) AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV35", name: "Byte string types", probe: Some("RETURN X'00FF' AS v"), expect: Some("X'00FF'"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV36", name: "Specified byte string minimum length", probe: Some("RETURN X'00' IS :: BYTES(1, 10) AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV37", name: "Specified byte string maximum length", probe: Some("RETURN X'00' IS :: BYTES(10) AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV38", name: "Specified byte string fixed length", probe: Some("RETURN X'00' IS :: BINARY(1) AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV39", name: "Temporal type support (renamed by Cor 1; was \"date, local datetime and local time support\")", probe: Some("RETURN date('2026-01-01') AS v"), expect: Some("DATE '2026-01-01'"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV40", name: "Temporal types: zoned datetime and zoned time support", probe: Some("RETURN zoned_datetime('2026-01-01T00:00:00Z') AS v"), expect: Some("DATETIME '2026-01-01T00:00:00.000000000Z'"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV41", name: "Temporal types: duration support", probe: Some("RETURN duration('P1D') AS v"), expect: Some("DURATION 'P1D'"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV45", name: "Record types", probe: Some("RETURN {a: 1} AS v"), expect: Some("{a: 1}"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV46", name: "Closed record types", probe: Some("RETURN {a: 1} IS :: RECORD {a INTEGER} AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV47", name: "Open record types", probe: Some("RETURN {a: 1} IS :: ANY RECORD AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV48", name: "Nested record types", probe: Some("RETURN {a: {b: 1}} AS v"), expect: Some("{a: {b: 1}}"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV50", name: "List value types", probe: Some("RETURN [1, 2] AS v"), expect: Some("[1, 2]"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::Limit("`||` on lists and `TRIM(list, n)` are not implemented.") },
    Feature { id: "GV51", name: "Specified list maximum length (Cor 1)", probe: Some("RETURN [1, 2] IS :: LIST<INTEGER>[2] AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV55", name: "Path value types", probe: Some("MATCH p = (a:Node {name: 'a'})-[:R]->(b) RETURN p"), expect: Some("<#1020, rel#4, #1021>; <#1020, rel#5, #1022>"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV60", name: "Graph reference value types", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV61", name: "Binding table reference value types", probe: None, expect: None, levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV65", name: "Dynamic union types", probe: Some("RETURN [1, 'a'] IS :: LIST<INTEGER | STRING> AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV66", name: "Open dynamic union types", probe: Some("RETURN 1 IS :: ANY AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV67", name: "Closed dynamic union types", probe: Some("RETURN 1 IS :: INTEGER | STRING AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV68", name: "Dynamic property value types", probe: Some("RETURN 1 IS :: PROPERTY VALUE AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV70", name: "Immaterial value types", probe: Some("RETURN null IS :: NULL AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV71", name: "Immaterial value types: null type support", probe: Some("RETURN null IS :: NULL AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV72", name: "Immaterial value types: empty type support", probe: Some("RETURN null IS :: NOTHING AS v"), expect: Some("false"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
    Feature { id: "GV90", name: "Explicit value type nullability", probe: Some("RETURN 1 IS :: INTEGER NOT NULL AS v"), expect: Some("true"), levels: None, diagnostics: Diagnostics::FromProbe, claimed: false, note: Note::None },
];

/// What one probe reached, and the condition it stopped with.
struct Verdict {
    parser: bool,
    semantics: bool,
    execution: bool,
    refusal: Option<GqlStatus>,
    /// The rendered result, when the probe ran.
    rows: Option<String>,
}

/// A result as [`Feature::expect`] spells it.
fn rendered(query: &str, result: &hydradb_cypher_engine::QueryResult) -> String {
    let mut rows: Vec<String> = result
        .rows
        .iter()
        .map(|row| row.values.iter().map(show).collect::<Vec<_>>().join(", "))
        .collect();
    if !query.contains("ORDER BY") {
        rows.sort();
    }
    rows.join("; ")
}

async fn verdict(query: &str) -> Verdict {
    let typed = parse_cypher25_syntax(query).is_ok()
        && parse_cypher25(query).is_ok_and(|document| matches!(document.statements.as_slice(), [Statement::Query(_)]));
    let engine = CypherEngine::new(graph());
    let parameters = ParameterMap::from([("x".to_string(), ScalarValue::Integer(1))]);
    let (semantics, execution, refusal, rows) = match engine.prepare_with_lists(query, &parameters, &ListParameterMap::new()) {
        Err(error) => (false, false, Some(error.gql_status()), None),
        Ok(prepared) => match engine.execute_prepared(&prepared).await {
            Err(error) => (typed, false, Some(error.gql_status()), None),
            Ok(result) => (typed, typed, None, Some(rendered(query, &result))),
        },
    };
    Verdict {
        parser: typed,
        semantics,
        execution,
        refusal,
        rows,
    }
}

fn mark(level: bool) -> &'static str {
    if level {
        "✅"
    } else {
        "❌"
    }
}

/// One row's four cells.
struct Cells {
    levels: [String; 3],
    diagnostics: String,
    refusal: Option<GqlStatus>,
    /// The probe's rendered result, when it ran.
    rows: Option<String>,
}

/// A probe that runs must give the standard's answer: running is not enough
/// for the Execution cell. A row whose levels are set by hand is exempt, since
/// a wrong answer is usually why it is marked ➖; its `expect`, if any, records
/// the standard's answer.
fn check_result(feature: &Feature, cells: &Cells) -> Result<(), String> {
    if feature.levels.is_some() {
        return Ok(());
    }
    match (&cells.rows, feature.expect) {
        (Some(rows), None) => Err(format!("{} runs and returns `{rows}`; give its expected result", feature.id)),
        (Some(rows), Some(expected)) if expected != rows => {
            Err(format!("{} returns `{rows}`, but the standard gives `{expected}`", feature.id))
        }
        _ => Ok(()),
    }
}

async fn cells(feature: &Feature) -> Cells {
    let verdict = match feature.probe {
        Some(query) => Some(verdict(query).await),
        None => None,
    };
    let refusal = verdict.as_ref().and_then(|verdict| verdict.refusal);
    let rows = verdict
        .as_ref()
        .filter(|verdict| verdict.execution)
        .and_then(|verdict| verdict.rows.clone());
    let levels = match (feature.levels, &verdict) {
        (Some(levels), _) => levels.map(str::to_string),
        (None, Some(verdict)) => [mark(verdict.parser), mark(verdict.semantics), mark(verdict.execution)].map(str::to_string),
        (None, None) => ["❌", "❌", "❌"].map(str::to_string),
    };
    let diagnostics = match (feature.diagnostics, refusal) {
        (Diagnostics::Judged(mark), _) => mark.to_string(),
        (Diagnostics::FromProbe, Some(status)) => format!("`{status}`"),
        (Diagnostics::FromProbe, None) if rows.is_some() => ledger_mark(feature.id).to_string(),
        (Diagnostics::FromProbe, None) => "—".to_string(),
    };
    Cells {
        levels,
        diagnostics,
        refusal,
        rows,
    }
}

/// A table cell, with `|` escaped.
fn cell(text: &str) -> String {
    text.replace('|', "\\|")
}

fn is_mandatory(feature: &Feature) -> bool {
    !feature.id.starts_with('G')
}

/// The family an optional feature's ID belongs to, and its heading.
fn family(id: &str) -> (&'static str, &'static str) {
    const FAMILIES: [(&str, &str); 14] = [
        ("GA", "Assorted semantics (GA)"),
        ("GB", "Lexical (GB)"),
        ("GC", "Catalog (GC)"),
        ("GD", "Data modification (GD)"),
        ("GE", "Expressions (GE)"),
        ("GF", "Functions (GF)"),
        ("GG", "Graph types (GG)"),
        ("GH", "External objects, undirected edges (GH)"),
        ("GL", "Literals (GL)"),
        ("GP", "Procedures (GP)"),
        ("GQ", "Query composition (GQ)"),
        ("GS", "Session commands (GS)"),
        ("GT", "Transactions (GT)"),
        ("GV", "Value types (GV)"),
    ];
    FAMILIES
        .into_iter()
        .find(|(prefix, _)| id.starts_with(prefix))
        .unwrap_or(("G", "Graph patterns and graph predicates (G0xx, G1xx)"))
}

const TABLE_HEAD: &str = "| ID | Feature | Parser | Semantics | Execution | Diagnostics | Notes |\n|---|---|---|---|---|---|---|\n";

const INTRO: &str = "\
# GQL feature matrix

> Generated by `query-language/gql/tests/engine/gql_features.rs`; do not edit by hand. Change the `FEATURES` table there and run `GQL_FEATURES_WRITE=1 just test-cypher-engine --test gql_features`.

Every mandatory construct and every optional feature of ISO/IEC 39075:2024 (GQL), with the feature IDs Technical Corrigendum 1:2026 adds, and how far the experimental engine takes each one.

";

const LEGEND: &str = "\
## Reading the matrix

| Column | ✅ means |
|---|---|
| Parser | Neo4j's Cypher 25 grammar accepts the text and `crates/cypher-parser-antlr/src/lower.rs` types it into the shared AST |
| Semantics | The engine's lowering and planning (`crates/cypher-engine/src/plan/`) accept it and apply its Syntax Rules |
| Execution | It runs and returns the result the standard's General Rules give, worked out from the books (`crates/cypher-engine/src/execute.rs`, `src/execute/functions.rs`) |
| Diagnostics | Every condition the books define for it — each Syntax Rule it can break, each exception it can raise — is pinned by a test (see [Rules](#rules)); ➖ means one is a gap or a deviation, and — that none can arise here. For a refused feature, the cell is the code it is refused with: `42001` when the grammar rejects the text, `0A000` when it parses but the engine does not execute it |

❌ is not done, ➖ is partial, and — is not applicable.

A row has a note only when it says something its cells cannot: a **deviation**, where a conforming query gets an answer other than the standard's; an **extension**, language the standard rejects that the engine also accepts; a **limit** on where a supported feature works; or the reason for a ➖ or — mark or a claim.

This file is generated. Every row comes from the `FEATURES` table in `query-language/gql/tests/engine/gql_features.rs`, and the Parser, Semantics, Execution and refusal cells come from running that row's probe query against the engine. The test fails when this file is not what the table renders. To change a row, edit the table and regenerate:

```bash
GQL_FEATURES_WRITE=1 just test-cypher-engine --test gql_features
```

`query-language/gql/tests/engine/gql_conformance.rs` holds the behaviour itself: results and the GQLSTATUS of every exception. Every difference from the books is in [deviations.md](deviations.md), the implementation-defined choices are in [README.md](README.md), and how to contribute is in [AGENTS.md](AGENTS.md).

";

/// IDs as `` `A` ``, `` `B` ``, comma-separated.
fn list(ids: &[&str]) -> String {
    ids.iter().map(|id| format!("`{id}`")).collect::<Vec<_>>().join(", ")
}

/// The summary paragraph for one part of the matrix.
fn summary(label: &str, rows: &[(&Feature, Cells)]) -> String {
    let with = |predicate: &dyn Fn(&Feature, &Cells) -> bool| -> Vec<&str> {
        rows.iter()
            .filter(|(feature, cells)| predicate(feature, cells))
            .map(|(feature, _)| feature.id)
            .collect()
    };
    let runs = with(&|feature, cells| !feature.claimed && cells.levels[2] == "✅");
    let partial = with(&|_, cells| cells.levels[2] == "➖");
    let diagnostics_only = with(&|_, cells| cells.levels[2] == "—" && cells.diagnostics == "✅");
    let claimed = with(&|feature, _| feature.claimed);
    let not_applicable = with(&|feature, cells| !feature.claimed && cells.levels[2] == "—" && cells.diagnostics != "✅");
    let not_supported = with(&|_, cells| cells.levels[2] == "❌");
    let refused = |code: &str| {
        rows.iter()
            .filter(|(_, cells)| cells.levels[2] == "❌" && cells.refusal.map(GqlStatus::code) == Some(code))
            .count()
    };
    let unprobed = rows
        .iter()
        .filter(|(feature, cells)| cells.levels[2] == "❌" && feature.probe.is_none())
        .count();
    let other_refusals = not_supported.len() - refused("42001") - refused("0A000") - unprobed;
    let mut text = format!("**{label} ({} rows):**\n\n", rows.len());
    let line = |text: &mut String, count: usize, what: &str, ids: Option<&[&str]>| {
        if count > 0 {
            let _ = match ids {
                Some(ids) => writeln!(text, "- {count} {what}: {}.", list(ids)),
                None => writeln!(text, "- {count} {what}."),
            };
        }
    };
    line(&mut text, runs.len(), "run", (label == "Optional").then_some(runs.as_slice()));
    let diagnosed = |mark: &str| {
        rows.iter()
            .filter(|(feature, cells)| !feature.claimed && cells.levels[2] == "✅" && cells.diagnostics == mark)
            .map(|(feature, _)| feature.id)
            .collect::<Vec<_>>()
    };
    let (pinned, partly, quiet) = (diagnosed("✅"), diagnosed("➖"), diagnosed("—"));
    let _ = writeln!(
        text,
        "  Of those, {} have every condition the books define pinned by a test, {} raise no condition here, and {} have a deviation or a gap in their conditions: {}.",
        pinned.len(),
        quiet.len(),
        partly.len(),
        list(&partly)
    );
    line(&mut text, partial.len(), "partial", Some(&partial));
    line(
        &mut text,
        diagnostics_only.len(),
        "supported at the diagnostics level",
        Some(&diagnostics_only),
    );
    line(&mut text, claimed.len(), "claimed", Some(&claimed));
    line(&mut text, not_applicable.len(), "not applicable here", Some(&not_applicable));
    if label == "Mandatory" {
        line(&mut text, not_supported.len(), "not supported", Some(&not_supported));
    } else {
        line(&mut text, not_supported.len(), "not supported", None);
        let _ = writeln!(
            text,
            "  Of those, {} are refused with `42001`, {} with `0A000`, {} with another code, and {} have no query form to probe.",
            refused("42001"),
            refused("0A000"),
            other_refusals,
            unprobed
        );
    }
    text.push('\n');
    text
}

fn rows_table<'a>(rows: impl IntoIterator<Item = &'a (&'a Feature, Cells)>) -> String {
    let mut table = TABLE_HEAD.to_string();
    for (feature, cells) in rows {
        let _ = writeln!(
            table,
            "| `{}` | {} | {} | {} | {} | {} | {} |",
            cell(feature.id),
            cell(feature.name),
            cells.levels[0],
            cells.levels[1],
            cells.levels[2],
            cells.diagnostics,
            cell(&feature.note.render())
        );
    }
    table
}

async fn render() -> String {
    let mut mandatory = Vec::new();
    let mut optional = Vec::new();
    let mut problems = Vec::new();
    for feature in FEATURES {
        let row = (feature, cells(feature).await);
        problems.extend(check_note(feature, &row.1).err());
        problems.extend(check_result(feature, &row.1).err());
        problems.extend(check_examined(feature, &row.1).err());
        if is_mandatory(feature) {
            mandatory.push(row);
        } else {
            optional.push(row);
        }
    }
    assert!(problems.is_empty(), "the matrix has problems:\n  {}", problems.join("\n  "));
    let mut text = INTRO.to_string();
    text.push_str(&summary("Mandatory", &mandatory));
    text.push_str(&summary("Optional", &optional));
    text.push_str(LEGEND);
    text.push_str(
        "## Mandatory constructs\n\nMinimum conformance (§24.2) is everything in Clauses 6 to 23 that no Conformance Rule assigns to an optional feature. The rows are derived from the standard's grammar: every production minimum conformance allows is covered by exactly one row (see [Mandatory grammar](#mandatory-grammar)). Rows are named by clause, and each probe uses only mandatory syntax.\n\n",
    );
    text.push_str(&rows_table(&mandatory));
    text.push_str("\n## Optional features\n\nAnnex D, Table D.1, plus the IDs Corrigendum 1 introduces, marked \"(Cor 1)\".\n");
    let mut sections: Vec<(&str, &str)> = Vec::new();
    for (feature, _) in &optional {
        let section = family(feature.id);
        if !sections.contains(&section) {
            sections.push(section);
        }
    }
    sections.sort_by_key(|(prefix, _)| if *prefix == "G" { String::new() } else { prefix.to_string() });
    for (prefix, heading) in sections {
        let _ = write!(text, "\n### {heading}\n\n");
        text.push_str(&rows_table(optional.iter().filter(|(feature, _)| family(feature.id).0 == prefix)));
    }
    text.push_str(RULES_INTRO);
    let count = |kind: fn(&Coverage) -> bool| RULES.iter().filter(|rule| kind(&rule.coverage)).count();
    let _ = writeln!(
        text,
        "{} rules: {} pinned by a test, {} extensions (non-conforming language also accepted), {} deviations, {} not applicable here, {} gaps, and {} pending for features that do not run yet.\n",
        RULES.len(),
        count(|coverage| matches!(coverage, Coverage::Test(_))),
        count(|coverage| matches!(coverage, Coverage::Extension(_))),
        count(|coverage| matches!(coverage, Coverage::Deviation(_))),
        count(|coverage| matches!(coverage, Coverage::NotApplicable(_))),
        count(|coverage| matches!(coverage, Coverage::Gap(_))),
        count(|coverage| matches!(coverage, Coverage::Pending(_))),
    );
    text.push_str("| Rule | Row | What it requires | How it is held |\n|---|---|---|---|\n");
    for rule in RULES {
        let held = match rule.coverage {
            Coverage::Test("probe") => "the row's probe".to_string(),
            Coverage::Test(name) => format!("test `{name}`"),
            Coverage::Deviation(text) => format!("Deviation: {text}"),
            Coverage::Extension(text) => format!("Extension (IE005): {text}"),
            Coverage::NotApplicable(text) => format!("Not applicable: {text}"),
            Coverage::Gap(text) => format!("Gap: {text}"),
            Coverage::Pending(text) => format!("Pending: {text}"),
        };
        let _ = writeln!(
            text,
            "| {} | `{}` | {} | {} |",
            rule.at,
            cell(rule.row),
            cell(rule.what),
            cell(&held)
        );
    }
    text.push_str(GRAMMAR_INTRO);
    for (subclause, production, row) in MANDATORY_GRAMMAR {
        let _ = writeln!(text, "| {subclause} | `{production}` | `{}` |", cell(row));
    }
    text
}

const RULES_INTRO: &str = "
## Rules

Every Syntax Rule that restricts what a query may say, and every rule that raises a condition, in Clauses 6 to 23, for mandatory and optional features alike; as Corrigendum 1 amends them, in our own words. A running row's Diagnostics cell comes from its rules here. The ledger is `query-language/gql/tests/engine/gql_features/rules.rs`.

";

const GRAMMAR_INTRO: &str = "
## Mandatory grammar

Every BNF production of Clauses 6 to 21 that minimum conformance allows, once every Conformance Rule of Clauses 6 to 23 is applied as Corrigendum 1 amends them, and the row above whose probe exercises it. Single characters (§21.4) are left out. How the list was derived is in `query-language/gql/tests/engine/gql_features/grammar.rs`.

| Subclause | Production | Row |
|---|---|---|
";

/// A running row's Diagnostics cell, from the rules ledger: ✅ when every
/// condition its rules define is pinned by a test, ➖ when one is a gap or a
/// deviation (the engine reports something other than the standard's
/// condition), and — when none can arise here.
fn ledger_mark(row: &str) -> &'static str {
    let rules: Vec<&Rule> = RULES.iter().filter(|rule| rule.row == row).collect();
    if rules
        .iter()
        .any(|rule| matches!(rule.coverage, Coverage::Gap(_) | Coverage::Deviation(_)))
    {
        "➖"
    } else if rules.iter().any(|rule| matches!(rule.coverage, Coverage::Test(_))) {
        "✅"
    } else {
        "—"
    }
}

/// A row that runs has been read against the books: the ledger has at least
/// one rule for it (`NotApplicable` when none can arise), and none of its
/// rules is still pending. A row that does not run has nothing to hold.
fn check_examined(feature: &Feature, cells: &Cells) -> Result<(), String> {
    if cells.levels[2] != "✅" || feature.claimed {
        return Ok(());
    }
    let rules: Vec<&Rule> = RULES.iter().filter(|rule| rule.row == feature.id).collect();
    if rules.is_empty() {
        return Err(format!(
            "{} runs, but the rules ledger has nothing for it: read its subclauses and add their rules",
            feature.id
        ));
    }
    if let Some(rule) = rules.iter().find(|rule| matches!(rule.coverage, Coverage::Pending(_))) {
        return Err(format!("{} runs now; {} is still pending", feature.id, rule.at));
    }
    Ok(())
}

/// Every rule names a row of the matrix and, when a test holds it, a test
/// that exists.
fn check_rules() {
    let conformance = include_str!("gql_conformance.rs");
    for rule in RULES {
        let Some(feature) = FEATURES.iter().find(|feature| feature.id == rule.row) else {
            panic!("{} names {}, which is not a row", rule.at, rule.row);
        };
        match rule.coverage {
            Coverage::Test("probe") => assert!(
                feature.expect.is_some(),
                "{} is held by the probe of {}, which has no expected result",
                rule.at,
                rule.row
            ),
            Coverage::Test(name) => assert!(
                conformance.contains(&format!("async fn {name}(")),
                "{} names the test {name}, which gql_conformance.rs does not have",
                rule.at
            ),
            _ => {}
        }
    }
}

/// Every table row has as many cells as its header: an unescaped `|` in a
/// cell would split it, and Markdown renderers then shift or drop cells.
fn check_tables(rendered: &str) {
    let cells = |line: &str| {
        let mut count = 0;
        let mut escaped = false;
        for character in line.chars() {
            match character {
                '\\' => escaped = !escaped,
                '|' if !escaped => count += 1,
                _ => escaped = false,
            }
            if character != '\\' {
                escaped = false;
            }
        }
        count - 1
    };
    let mut header = None;
    for (number, line) in rendered.lines().enumerate() {
        if !line.starts_with('|') {
            header = None;
            continue;
        }
        let count = cells(line);
        match header {
            None => header = Some(count),
            Some(expected) => assert_eq!(
                count,
                expected,
                "features.md line {} has {count} cells, not {expected}: {line}",
                number + 1
            ),
        }
    }
}

/// Every mandatory production has exactly one row, and that row exists and is
/// mandatory.
fn check_grammar() {
    let mut productions = std::collections::BTreeSet::new();
    for (_, production, row) in MANDATORY_GRAMMAR {
        assert!(productions.insert(production), "{production} is listed twice");
        assert!(
            FEATURES.iter().any(|feature| feature.id == *row && is_mandatory(feature)),
            "{production} names {row}, which is not a mandatory row"
        );
    }
}

#[tokio::test]
async fn the_feature_matrix_is_generated_from_the_engine() {
    let mut seen = std::collections::BTreeSet::new();
    for feature in FEATURES {
        assert!(seen.insert(feature.id), "{} has two rows", feature.id);
    }
    check_grammar();
    check_rules();
    let rendered = render().await;
    check_tables(&rendered);
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../query-language/gql/features.md");
    if std::env::var_os("GQL_FEATURES_WRITE").is_some() {
        std::fs::write(path, &rendered).unwrap_or_else(|error| panic!("{path}: {error}"));
        return;
    }
    let committed = std::fs::read_to_string(path).unwrap_or_else(|error| panic!("{path}: {error}"));
    if committed != rendered {
        let first = committed
            .lines()
            .zip(rendered.lines())
            .position(|(committed, rendered)| committed != rendered)
            .unwrap_or_else(|| committed.lines().count().min(rendered.lines().count()));
        panic!(
            "query-language/gql/features.md is not what the FEATURES table renders (first difference at line {}):\n  committed: {}\n  rendered:  {}\n\
             Regenerate it: GQL_FEATURES_WRITE=1 just test-cypher-engine --test gql_features",
            first + 1,
            committed.lines().nth(first).unwrap_or("<end of file>"),
            rendered.lines().nth(first).unwrap_or("<end of file>"),
        );
    }
}
