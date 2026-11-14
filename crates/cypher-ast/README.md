# hydradb-cypher-ast

Parser-independent Cypher syntax model shared by HydraDB's experimental
frontends. This crate deliberately has no parser, planner, storage, or
execution dependencies.

```text
ANTLR Cypher 25 parser ─────┐
libcypher/openCypher ───────┼──> hydradb-cypher-ast
future parser ──────────────┘
```

The AST keeps dialect/version provenance and syntax-level graph patterns. A
later lowering layer erases dialect differences into `hydradb-graph-ir`.
