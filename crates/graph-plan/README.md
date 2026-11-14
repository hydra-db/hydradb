# hydradb-graph-plan

HydraDB's small, storage-aware planning boundary:

```text
shared Cypher AST
        |
        v
GraphLogicalPlan       semantic graph meaning
        |
        v
GraphPhysicalPlan      exact KV seeks, scans, filters, and projections
```

Logical and physical nodes are distinct plain Rust enums in one crate. The
physical plan is intentionally HydraDB-specific and is the exact value rendered
by EXPLAIN and consumed by `GraphShard`. There is no plugin framework or
general execution trait hierarchy.
