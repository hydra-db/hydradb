# Changelog

All notable changes to HydraDB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-11-06

### Added

- **ISO GQL query surface.** The standard GQL dialect is parsed and planned
  alongside OpenCypher, sharing one engine and one storage path.
- **Bolt proxy.** A standalone proxy routes Bolt 5.x traffic across query nodes
  without linking the native graph libraries.
- **MCP server.** A Model Context Protocol server exposes graph queries to agent
  clients.
- **DataFusion execution.** A DataFusion-backed Cypher path, including list and
  map element access.
- **FalkorDB import.** A dedicated path for importing a FalkorDB dump into an
  object-store image.
- **Query telemetry.** Per-query telemetry with OTLP export in the server
  runtime.

### Changed

- **Experimental Cypher engine.** Batched vertex identity seeks, concurrent
  UNION arms, ordered relationship seeding, and bound-edge endpoint pushdown.
- **Read path.** Owner strong reads and firmer writer fencing on takeover.
- **Graph data science.** Degree and traversal procedures exercised at
  100M-edge scale.

### Notes

Exclusions are unchanged from 0.3.0 (internal documentation, benchmark and
parity scripts, test suites, the Jepsen suite, the Quint specification models,
the Typst/interactive textbook, CI workflows, and editor/agent tooling).

## [0.3.0] - 2026-10-22

### Added

- **FalkorDB import.** A bounds-checked decoder for FalkorDB Redis `DUMP`
  payloads (`falkordb-rdb`) and a deterministic identity and scope resolver
  (`falkor-dump-import`), with `inspect` / `scan` and `plan` / `import` tools for
  migrating an existing FalkorDB graph into an object-store image.
- **Offline graph images.** A canonical key/value encoder for building a durable
  graph image out of band, emitting only durable graph state for bulk load and
  import.
- **Published planner statistics.** Per-(scope, cell) cardinality counts the
  indexer derives from index keys alone and publishes next to the store, so the
  planner can cost reads without a dedicated statistics key. Backed by a new
  lease-owned, forward-only object-publication protocol (`object-publication`).
- **Degree procedure.** `CALL algo.degree(...)` returns the relationship degree
  of every vertex with a label from compiled topology, without materialising
  each relationship record.
- **Engine capability checks.** The isolated Cypher engine can now report
  whether it can run a logical or physical plan without opening storage, so a
  router can decide before committing a request to it.
- **Create-only PUT conformance.** Object stores are checked for honouring
  create-only PUTs, the primitive WAL fencing relies on, so a non-conforming
  S3-compatible layer is reported rather than silently clobbering an
  acknowledged WAL object.

### Changed

- **Test code fully excluded.** The inline test modules that still shipped in
  0.2.0 are gone; the public tree is now source only, matching the exclusion
  policy below.

### Notes

Exclusions are unchanged from 0.2.0 (internal documentation, benchmark and
parity scripts, test suites, the Jepsen suite, the Quint specification models,
the Typst/interactive textbook, CI workflows, and editor/agent tooling). As of
this release the test suites are excluded in full.

## [0.2.0] - 2026-09-28

Initial public release of HydraDB, an object-store-native distributed graph
database written in Rust. S3-compatible object storage is the durable source of
truth; query nodes and indexers hold only disposable local state, so they can be
replaced or scaled without moving the graph.

### Added

- **Object-store durability.** Graph records, WALs, manifests, and immutable
  traversal indexes persist to S3-compatible storage on SlateDB.
- **Snapshot-consistent reads.** Every query runs against one pinned SlateDB
  snapshot; indexed traversal combines a compiled CSC generation with its
  visible WAL overlay.
- **OpenCypher queries.** A practical OpenCypher subset for graph reads and
  mutations, plus native path procedures.
- **Graph-native execution.** Planner support for property indexes, reverse
  adjacency, sparse traversal, and SuiteSparse GraphBLAS where appropriate.
- **Familiar clients.** Neo4j-compatible connectivity over Bolt 5.x and a typed
  JSON / streaming NDJSON HTTPS query API.
- **Safe writer handoff.** Object-store CAS leases select the active writer per
  cell; SlateDB writer epochs fence stale writers.
- **Independent compute.** Query nodes and indexers scale separately and rebuild
  their local caches from durable state.
- **Bounded server runtime.** Authentication, authorization, deadlines, result
  limits, backpressure, cancellation, cache budgets, metrics, and traces.
- **Deployment.** Dockerfile for the `graph-node` and `graph-indexer` images and
  a Helm chart for Kubernetes; MinIO-based local harnesses.
- **Docs.** `README.md`, `USING-HYDRADB.md`, and `architecture.md`.

### Notes

This is a public source snapshot. Internal-only material is intentionally
excluded: internal documentation (bug post-mortems, experiment/plan/review/runbook
notes, formal-methods notes), benchmark scripts, test suites, the Jepsen suite,
the Quint specification models, the Typst/interactive textbook, CI workflows, and
editor/agent tooling configuration. Full source and the three public-facing docs
above are included.

[Unreleased]: https://github.com/hydra-db/hydradb/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/hydra-db/hydradb/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/hydra-db/hydradb/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/hydra-db/hydradb/releases/tag/v0.2.0
