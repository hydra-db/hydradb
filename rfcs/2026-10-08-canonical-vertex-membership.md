---
title: Canonical vertex membership and lifecycle
status: proposed
date: 2026-10-08
issue: 227
authors:
  - MagellaX
implemented_in:
supersedes:
---

## Summary

Make `cell/{cell}/vertex/{id:020}` the durable authority for vertex membership within a graph scope, cell, and storage snapshot. A valid record with empty metadata denotes an existing vertex; a missing record denotes absence. Metadata removal preserves membership, explicit deletion removes it, and edge creation establishes implicit endpoints in the same transaction as the edge. Bind Cypher entities against that authority across the legacy row executor, graph-plan adapter, and experimental engine. Introduce the contract through an unstable opt-in and a coordinated, resumable backfill for existing cells before changing their query semantics.

## Motivation

This addresses a documented limitation rather than a newly discovered regression. On main at `e0f50952103b359c91580529349b5cdf64301840`, [USING-HYDRADB.md](https://github.com/hydra-db/hydradb/blob/e0f50952103b359c91580529349b5cdf64301840/USING-HYDRADB.md#L150-L190) describes ID-only matches that can return an ID never created in the graph. On an empty cell, `MATCH (n {id: 777001}) RETURN n.id` can yield `777001`; adding `:Probe` makes the match empty.

The same discrepancy can be reproduced after actual creation and deletion:

```cypher
CREATE (n:Probe {id: 777001})-[:LINK]->(m:Probe {id: 777002})
MATCH (n:Probe {id: 777001}) DETACH DELETE n
MATCH (n {id: 777001}) RETURN n.id
```

The desired final result is zero rows. The current ID candidate path supplies the ID, and missing metadata becomes `VertexMetadata::default()` during hydration. A predicate-free pattern then accepts the candidate. Count and window shortcuts also need attention; repairing only the general hydration loop would leave incorrect results in optimized paths. See [candidate selection and hydration](https://github.com/hydra-db/hydradb/blob/e0f50952103b359c91580529349b5cdf64301840/src/shard/query.rs#L5844-L5992) and [predicate matching](https://github.com/hydra-db/hydradb/blob/e0f50952103b359c91580529349b5cdf64301840/src/shard/query.rs#L10789-L10822).

Storage has the opposite ambiguity. Setting empty metadata on a missing ID is treated as unchanged; clearing the last attributes deletes its vertex key. Plain native edge writes need not persist either endpoint, while the experimental all-vertex scan enumerates vertex keys. An ID seek, a metadata scan, and topology can therefore disagree about which nodes exist. See [metadata updates](https://github.com/hydra-db/hydradb/blob/e0f50952103b359c91580529349b5cdf64301840/src/shard/write.rs#L659-L809), [record removal](https://github.com/hydra-db/hydradb/blob/e0f50952103b359c91580529349b5cdf64301840/src/shard/write.rs#L7292-L7313), and [the vertex scan](https://github.com/hydra-db/hydradb/blob/e0f50952103b359c91580529349b5cdf64301840/src/shard/query.rs#L6753-L6780).

Leaving this unchanged preserves the documented limitation, but every additional executor must reproduce the same inference rules. A canonical membership contract removes that ambiguity without making asynchronously built indexes authoritative.

## Goals and non-goals

Goals:

- Preserve existing empty vertices independently of labels, properties, and incident edges.
- Make supported Cypher entity bindings, counts, and scans agree at one pinned snapshot.
- Cover native writes, bulk and segment imports, and explicit and detach deletion.
- Specify upgrade, backfill, retry, and failure behavior before implementation.

Non-goals:

- Replace SlateDB writer fencing, WAL durability, or snapshot management.
- Replace GraphBLAS, CSC generations, topology overlays, or raw traversal APIs.
- Add currently unsupported standalone-node CREATE or node-only query shapes to the legacy engine.
- Introduce selective property indexing or optimize metadata-index diffs in this contribution.
- Recover historical isolated vertices for which no durable evidence survives.

## Design

### Membership invariant

For an activated cell and snapshot `S`, `exists(id, S)` is true exactly when its canonical vertex key contains valid encoded metadata at `S`. `Some(VertexMetadata::default())` is an empty vertex; `None` is absent. Malformed bytes are a storage error, never absence. Cell and graph scope are part of the identity.

Every committed live edge has canonical records for both endpoints at the same snapshot. Removing attributes or the last incident edge does not remove either endpoint. Only explicit vertex deletion removes membership. `DELETE` retains the existing connected-node conflict rule; `DETACH DELETE` removes membership and incident relationships atomically within the existing transaction boundary.

Use the current `vertex-metadata-v1` encoding. Its header-only value, `vertex-metadata-v1\n`, already decodes as empty metadata; a zero-byte value is invalid. No second membership key per vertex is needed. See [codec](https://github.com/hydra-db/hydradb/blob/e0f50952103b359c91580529349b5cdf64301840/src/codec.rs#L424-L489).

### Presence-aware reads

Add internal single and batch reads returning `Option<VertexMetadata>`, including the input ID for each batch result. Keep the existing public default-returning metadata accessor for compatibility, document that it cannot establish existence, and route entity binding through the presence-aware form. A batch result retains explicit absence rather than silently manufacturing an empty record.

Read membership and cell readiness through the retained snapshot already used by the query. A query-local cache may hold positive and negative results keyed by cell and ID because its lifetime is restricted to that snapshot; its resident bytes count against existing query budgets. Do not introduce a process-wide negative membership cache. Old snapshots continue to see the membership and readiness that existed when they were pinned, including across delete and recreate.

Expected files: `src/core/state.rs`, `src/shard/query.rs`, `src/shard/graph_plan.rs`, and `src/shard/experimental_cypher.rs`. Both storage adapters must preserve absence when translating hydrated records into engine entities.

### Entity binding

An ID literal or parameter is a candidate constraint, not proof of membership. Filter absent IDs before emitting bound entities, aggregating, ordering, applying row windows, or executing MATCH-driven mutations. Apply the same rule to repeated MATCH clauses and supported OPTIONAL MATCH shapes, preserving their null-extension behavior.

Audit the general row matcher, graph-kernel ID and count shortcuts, page/window shortcuts, graph-plan `VertexIdSeek`, and experimental seek and hydration routes. A zero-hop Cypher path may bind only an existing vertex. Raw kernel APIs may continue accepting arbitrary seed IDs; changing their reachability convention is outside this RFC.

Positive-hop results may use effective live topology as proof of endpoint membership only in a complete cell written under the atomic endpoint invariant. Any path lacking that proof performs presence-aware hydration. Label/property indexes remain candidate generators with existing residual verification. All-vertex scans use canonical vertex keys at the query snapshot. This corrects scans the experimental engine already supports; enabling a new legacy all-vertex query shape is a separate PR.

### Creation and metadata updates

Track previous presence separately from previous metadata in single, batch, merge, import, precheck, and transaction-local overlay paths. In the new mode, native metadata setters retain their upsert meaning: setting or merging default metadata into an absent ID creates a header-only record. A present vertex whose metadata is unchanged remains a no-op. Removing its final attribute writes the empty record and removes only derived label/property entries.

Supported `CREATE` and `MERGE` shapes may establish entities. The current legacy route rejects standalone-node CREATE; adding that syntax is separate from preserving empty native vertices and endpoints of supported edge patterns. A mutation reached through `MATCH` must first bind existing entities; it must not upsert an absent ID merely because a setter can do so. Preserve concurrent-write validation and transaction overlay visibility so a newly created empty vertex is visible to subsequent clauses in the same transaction.

### Native endpoints and imports

Establish missing endpoints without replacing existing metadata in the same transaction that publishes the edge or relationship. Cover `write_edge`, edge batches, relationship creation/merge, metadata-bearing import paths, trusted bulk edge imports, and outbound segment imports, including self-loops and destination-only vertices.

For multi-chunk import APIs, the invariant holds for each committed chunk; this does not promise file-wide atomicity beyond the existing API. A failed chunk publishes neither its new edges nor its endpoint records. Endpoint deduplication and bounded chunk sizes must preserve existing memory and transaction limits.

Preserve existing idempotency semantics. Replaying a previously committed request returns its original result without mutating the graph, even if an endpoint was subsequently deleted. It must not resurrect the endpoint. A fresh idempotency key represents a new create/import operation and can recreate endpoints. This follows the existing [segment retry contract](https://github.com/hydra-db/hydradb/blob/e0f50952103b359c91580529349b5cdf64301840/src/shard/write.rs#L5870-L5896).

### Explicit deletion

Split metadata updating from canonical vertex removal in `src/shard/write.rs`. The delete helper removes label/property entries and the vertex record, while the metadata update helper always preserves or creates the record in the new mode. Determine `vertex_deleted` from previous presence, not whether previous attributes were nonempty. The first deletion of an empty isolated vertex reports true; a new deletion request for the now-absent vertex reports false. Replaying the first request returns its original result.

Retain current incident-edge isolation checks, detach transaction atomicity, relationship accounting, and idempotency records. The current [delete path](https://github.com/hydra-db/hydradb/blob/e0f50952103b359c91580529349b5cdf64301840/src/shard/write.rs#L1831-L1868) uses the metadata update helper, so changing that helper alone would accidentally preserve explicitly deleted vertices.

### Cell readiness

Introduce the opt-in setting `unstable_canonical_vertex_membership`, default false, and a durable per-cell readiness record at `cell/{cell}/vertex_membership`. The setting enables the new write contract; the snapshot-visible readiness record gates the new read contract. Its first format is `vertex-membership-v1`, with explicit `backfilling` and `complete` states. A missing record is legacy. An unknown version or malformed record is an error for a node configured to use this feature.

For a proven virgin cell, create `complete` atomically with its first graph transaction. Absence of vertex keys alone is not proof: an edge-first cell can have topology but no metadata. If the existing cell-creation path cannot prove virginity, require initialization through the migration command. No readiness decision performs an unbounded scan on an ordinary query.

During backfill, readers retain legacy behavior; a partial vertex prefix must not be advertised as complete membership. Read the readiness record at the same snapshot as graph data. At the final commit, publish `complete` only after coverage verification. Requests already holding an older snapshot continue with its legacy readiness state. New binaries reject writes to a cell carrying this readiness record when the unstable setting is disabled, and reject reads of a complete cell in that configuration; turning off one node's setting must not silently restore legacy behavior for activated data.

### Backfill

The first migration is a maintenance operation, `unstable-backfill-vertex-membership`, under exclusive application writer ownership and SlateDB's existing writer fencing. Pause normal writes across the graph store, exclude old binaries from writer credentials/routing, and upgrade serving readers before activation. Backfill does not invent a separate lease protocol.

Enumerate surviving canonical vertex records and effective live canonical topology across every edge type. Resolve outbound segments and tombstones before selecting endpoints; include incoming-only destinations, self-loops, and live relationship records. Do not infer completeness from CSC indexes, degree counters, or the topology xlog. Indexes can lag, and standalone vertex lifecycle events are not represented in an edge-only log. Old deleted edges must not resurrect their endpoints.

Use bounded chunks. Persist migration version, current scan phase, continuation key, and the last committed storage sequence together with each chunk's endpoint writes in the readiness record. On restart, resume only after validating the checkpoint and exclusive ownership; unexpected intervening application writes require abort and a new verification pass. Skip present records rather than replacing their metadata. Repeating a chunk is idempotent.

Perform a complete verification pass under the same maintenance ownership before publishing `complete`. Writer fencing, cancellation, a failed chunk, or failed verification leaves the cell in `backfilling`; it must never advance readiness optimistically. Resume or restart the maintenance operation after the failure is resolved.

Previously unrecorded isolated vertices cannot be reconstructed if neither a surviving vertex record nor a live incident edge identifies them. A cleared legacy record is likewise ambiguous. State this limitation in the migration report and operator documentation; require application re-import if those vertices are needed. Do not derive existence from arbitrary IDs previously queried.

## Format compatibility

Vertex metadata remains `vertex-metadata-v1`; its header-only empty value is already readable by the current decoder. The new cell readiness record is version 1 of a new format. SlateDB manifests/WAL, CSC generations, lease records, and Bolt/HTTP envelopes do not change. Query results and deletion statistics intentionally change when a cell becomes complete and the new mode is enabled.

Older binaries can decode new empty metadata, but their writers can remove those records and their readers retain the old query semantics. Therefore a mixed-version rolling upgrade must not activate the feature. Upgrade all writers/readers and exclude old writers before backfill. Keep legacy cells in legacy mode until complete; do not switch the whole store solely from a process setting.

Downgrading an activated cell to old writers is not a supported rollback: the first old metadata clear or edge-first import can violate membership. Disabling the feature does not convert data back or preserve empty-vertex semantics. To roll back safely, stop writes and restore a pre-activation backup, acknowledging that this discards later commits, or build and test a separate conversion procedure. Deployment documentation must identify the activation commit as the compatibility boundary.

## openCypher and Bolt conformance

For supported query shapes, expect absent-ID MATCH to return zero rows, its aggregate count to be zero, and OPTIONAL MATCH to null-extend according to the existing clause semantics. Empty creation and removal of final labels/properties retain an entity; explicit DELETE removes it. Include deletion/recreation and zero-hop node binding in the conformance comparison. Select the relevant vendored TCK scenarios during implementation and report their exact before/after results; this RFC does not claim improved TCK pass counts yet.

HydraDB's existing `id` convention remains unchanged. Neo4j drivers observe the corrected rows and deletion counters through existing Bolt messages; no new handshake, protocol version, bookmark encoding, or HTTP response field is required. Keep supported query results consistent across available execution routes without broadening unsupported syntax silently.

## Operations

Performance effects are estimates; there is no implementation benchmark yet. Native edge-first imports add endpoint puts, deduplicated within bounded chunks. Existing endpoints should avoid redundant writes. ID-only entity queries can add canonical point reads; batch hydration and query-local caching amortize them. Existing positive-hop topology proof can avoid unnecessary endpoint reads after readiness. Empty isolated vertices add durable vertex records and scan entries. Backfill performs bounded canonical reads and writes proportional to surviving membership and live topology, independent of index freshness.

Charge membership cache entries and temporary endpoint sets to existing resident-byte/transaction budgets. Report migration phase, processed keys, created records, verification failures, checkpoint sequence, and activation state in progress output and structured logs. Operators use failures to resume the migration or correct writer isolation; they never force `complete` to bypass verification. The unstable setting remains false for deployments that have not chosen the coordinated migration.

## Testing

### Current characterization

Five temporary native characterization tests passed against the unchanged main revision above, in a separate checkout copy with only the test harness appended. They record current behavior; they are not desired-behavior regression tests or an implementation of this RFC. The command was `just test --features experimental-cypher-engine vertex_membership_characterization -- --nocapture`, on Debian 12 with Rust 1.99.0, libcypher-parser 0.6.2, and SuiteSparse GraphBLAS 7.4.0. The harness and output are included in the tracking issue.

| Characterization | Observed current behavior |
| --- | --- |
| Empty codec | Header-only metadata is valid; zero-byte metadata is invalid |
| Metadata lifecycle | Default set on absence writes no record; clearing final metadata deletes the record; subsequent native deletion reports false |
| Legacy entity lookup | Absent and explicitly deleted IDs each return one row; labelled absence returns none; absent-ID count is one |
| Native edge-first write | Neither endpoint has a vertex record; a legacy traversal still returns the destination |
| Experimental lookup/scan | An edge-first graph's vertex scan returns zero rows; an absent-ID seek returns one |

`just ci` passed native dependency discovery, formatting, default Clippy, and chaos-feature Clippy, then failed at `clippy-opencypher` on unchanged source under Rust 1.99.0: deprecated `AtomicU64::fetch_update` in `src/shard/query.rs:8925` and `clippy::double_must_use` diagnostics generated by `async_trait` in `src/query/coordination.rs:1647`. Later CI stages did not run. The fork has no GitHub Actions workflows configured. This documentation-only PR does not repair those independent lint failures and does not claim a green full CI run.

### Implementation acceptance

| Area | Required assertions |
| --- | --- |
| Presence and codec | Missing, valid empty, nonempty, and corrupt records remain distinct; batch reads preserve absence and input identity |
| Creation | Single/batch setters, merge prechecks, imports, and Cypher CREATE/MERGE persist empty vertices; repeated unchanged writes are no-ops |
| Metadata removal | Removing final label/property preserves membership and cleans derived indexes |
| Edge lifecycle | All native/relationship/bulk/segment paths establish endpoints atomically; deleting the last edge preserves them |
| Explicit deletion | Empty and populated vertices delete once; connected DELETE conflicts; DETACH removes incidents and membership in one commit |
| Replay | Identical idempotency keys do not resurrect deleted entities; fresh keys can recreate them |
| Entity binding | Absent ID, count, supported OPTIONAL MATCH, LIMIT/SKIP, repeated MATCH, and zero-hop paths agree across supported routes |
| Snapshots | Retained snapshots before/after create, clear, delete, recreate, and activation see their own state; reopen sees committed membership |
| Topology variants | Destination-only endpoints, self-loops, multiple types, outbound-only policy, lagging/absent CSC, and tombstoned segments |
| Migration | Empty/legacy cells, interrupted chunks, repeated chunks, restart, verification failure, old-writer exclusion, and no partial activation |
| Isolation and budgets | Multiple scopes/cells do not leak membership; chunk and query memory budgets hold |

Retain actual snapshot handles in tests; the current API does not reopen arbitrary historical epochs. Inject failure before/after endpoint publication and storage commit, including writer fencing. Compare row multisets across supported routes and existing optimized/unoptimized plans. Run `just ci`, `just test-experimental-cypher`, and relevant storage/chaos harnesses for the implementation; measure cold/warm seeks and edge-first import throughput before rollout. A documentation PR cannot satisfy these implementation acceptance gates.

## Rollout

Use small implementation PRs against the tracking issue:

1. Add characterization and failure-oriented desired-behavior coverage, and presence-aware helpers without activating new semantics.
2. Add the unstable write contract, explicit deletion primitive, endpoint creation across all native/import paths, and gated entity binding across the three routes. Keep production defaults unchanged.
3. Add bounded offline backfill, durable readiness, restart/fencing tests, and operator documentation. Initialize proven virgin cells through the same contract.
4. Validate the acceptance matrix, TCK delta, resource budgets, and performance; activate only on opted-in complete cells. Move this RFC to `accepted/` only once the tested implementation is on main.

Coordinate zero-hop binding with [PR #150](https://github.com/hydra-db/hydradb/pull/150), optimizer parity tests with [PR #162](https://github.com/hydra-db/hydradb/pull/162), and labelled endpoint creation with [PR #179](https://github.com/hydra-db/hydradb/pull/179). This proposal does not duplicate the streaming work in [PR #211](https://github.com/hydra-db/hydradb/pull/211).

## Alternatives

- **A separate existence record per vertex:** duplicates membership and metadata publication, adds reads/writes, and requires another atomic invariant when the existing encoding can represent an empty entity.
- **Treat every queried ID as present:** retains phantom MATCH rows and makes explicit deletion unable to remove identity.
- **Infer membership from labels/properties:** loses empty and edge-first vertices and makes attribute removal equivalent to deletion.
- **Infer membership from CSC or the topology xlog:** misses isolated vertices and can depend on asynchronous index freshness; the xlog records topology changes rather than all vertex lifecycle events.
- **Backfill on reads:** introduces unbounded request work, makes absence depend on access order, and cannot distinguish a missing legacy endpoint from an absent ID without full coverage.
- **Immediate default-on change:** exposes old data's incomplete vertex prefix and permits old writers to violate the new contract during rolling upgrades.
- **Do nothing:** preserves the documented compatibility limitation and requires every executor to maintain inconsistent existence rules.

## Open questions

The design recommends one storage representation, replay policy, and initial maintenance migration. Maintainers need to review whether that migration burden is acceptable for the next release and coordinate the overlapping query PRs. An online migration or additional legacy query support would require a follow-up design; neither is a prerequisite silently left to the implementer here.

## Updates

- 2026-10-08: Initial proposal based on main `e0f50952103b359c91580529349b5cdf64301840`. Follows the RFC structure and issue/PR sequence proposed in [contribution-guide PR #100](https://github.com/hydra-db/hydradb/pull/100); that guide is not merged on this revision.
