# hydradb-cypher-parser-antlr

HydraDB's side-by-side, pure-Rust Cypher frontend. It does not replace the
existing `libcypher-parser` query path yet.

```text
Cypher 25 text
      |
      v
Neo4j Cypher25Lexer.g4 + Cypher25Parser.g4
      |
      v
ophi antlr-rust-codegen
      |
      v
validated, typed Rust syntax tree (`parse_cypher25_syntax`)
      |
      v
parser-independent owned AST (`parse_cypher25`)
      |
      v
common graph IR -> optimizer -> execution   (later)
```

## Grammar source

The source grammars are the repository copies in
`query-language/cypher/grammar/Cypher25Lexer.g4` and
`query-language/cypher/grammar/Cypher25Parser.g4`, sourced from Neo4j's
`cypher-language-support` repository. `build.rs` generates the Rust lexer and
parser into Cargo's `OUT_DIR` on build.

The source `.g4` files are never rewritten. The build stages a deterministic
copy with two generator-only compatibility transforms:

- unused Neo4j semantic type arguments such as `parameter["INTEGER"]` are
  removed because ophi 0.34 does not accept string-valued rule arguments;
- the grammar token name `METADATA` becomes `METADATA_KW` because ophi emits a
  Rust static named `METADATA`. Its matched query text remains `METADATA`.

It then applies the GQL overlay in `query-language/gql/overlays/`: ISO GQL syntax Neo4j's
grammar lacks, such as `EXISTS ( pattern )`, double-quoted identifiers,
doubled quotes in strings, abbreviated edges and binary literals. Each item
replaces, extends or inserts after a named rule of the staged copy, and cites
the GQL subclause it implements. An item whose rule the grammar no longer has
fails the build, so a grammar upgrade cannot silently drop one. Every item only
adds syntax: text Cypher 25 parses keeps its parse.

Cargo watches the active grammar and overlay inputs directly through
`rerun-if-changed`. Repository workflows include `query-language/**` in their path filters,
so grammar-only changes run the relevant PR checks. Parser tests run in the
unfiltered CI workflow as well.

## Use

```rust
let document = hydradb_cypher_parser_antlr::parse_cypher25(
    "MATCH (p:Product) LET taxed = p.price * 1.18 RETURN taxed",
)?;

println!("{:?}", document.dialect);
# Ok::<(), hydradb_cypher_parser_antlr::Cypher25ValidationError>(())
```

Use `parse_cypher25_syntax` when a caller needs the complete generated tree.
The shared AST has an explicit owned `SyntaxFragment` fallback for valid
constructs whose typed adapter coverage has not landed yet.

Run `just test-cypher` from the repository root to regenerate, lint, and test
the frontend.
