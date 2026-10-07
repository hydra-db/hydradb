# Unstable canonical vertex membership

The opt-in `GraphOpenOptions::unstable_canonical_vertex_membership` (graph-node
and graph-indexer:
`GRAPH_UNSTABLE_CANONICAL_VERTEX_MEMBERSHIP=true`) makes valid canonical vertex
records authoritative for membership. An empty record is an existing vertex;
missing records are absent. Clearing labels/properties preserves membership.
Native edge and relationship writers establish missing endpoints atomically.
Only explicit vertex deletion removes a record. Removing the last edge does
not remove either endpoint. The default remains false.

Queries adopt this contract only when their pinned storage snapshot contains
the cell's completed readiness record. Existing cells, including empty cells,
must be initialized through maintenance: absence of metadata does not prove
that an edge-first cell is new. Legacy all-vertex query support is unchanged;
the experimental engine's existing scan includes empty vertices after migration.
Raw traversal kernels retain their seed conventions.

## Upgrade and initialize

1. Back up the graph store. Upgrade all serving readers, writers, and indexers
   to a binary with this option enabled, while cells still have legacy readiness.
2. Stop **all writers across the graph store**, including index/statistics
   publication. Exclude older binaries from routing and writer credentials.
   Do not activate during a mixed-version rolling upgrade.
3. Configure the normal object-store environment (`CLOUD_PROVIDER`, `LOCAL_PATH`
   for local stores, or the cloud credentials). Use the same graph store path
   used by the application, not a graph display name.
4. Run `just unstable-backfill-vertex-membership <store-path> <cell>` for each
   cell. This takes the existing SlateDB writer; no additional lease is used.
5. Resume serving writers only after every intended cell reports `complete=true`.

The library API `unstable_backfill_vertex_membership` supports bounded calls
with `max_records_per_commit`, `max_bytes_per_commit`, and `max_commits`. Defaults
are 256 records/destinations and 8 MiB per commit, up to 64 commits per call.
The command uses one commit per call and prints cumulative progress. A large
individual record requires a larger explicit library byte budget. Canonical
and endpoint reads share that budget; reduce the record count or raise the
explicit byte budget if a chunk exceeds it. Encoded
segments are read as whole records; their destination processing is bounded
and checkpointed. Cancellation rolls back the current uncommitted chunk.

Maintenance scans surviving canonical vertices, outbound edges, outbound
segments resolved against tombstones, and relationship records across all edge
types. It preserves existing metadata and includes incoming-only destinations
and self-loops. CSC indexes, degree counters, and the topology xlog are not
membership authorities. A separate full verification pass precedes activation.
Each durable chunk stores its phase, continuation key/segment offset, counters,
and storage sequence together with endpoint records. Partial migration retains
legacy query semantics and rejects ordinary writes to that cell.

Restart the same command after an interruption. An unexpected storage sequence
change indicates that exclusive maintenance ownership was broken; stop the
other writers and run with `--restart` to repeat scanning and verification.
Unknown checkpoint formats, malformed records, writer fencing, and verification
failure abort instead of forcing readiness. Correct the underlying issue and
resume/restart. Already committed chunks remain durable. Completed cells cannot
be deactivated with `--restart`.

The report always exposes `legacy_isolated_vertices_may_be_missing=true`:
isolated legacy vertices whose records were never stored or were cleared
cannot be reconstructed from surviving data. Re-import them from an application
source if required; previously queried arbitrary IDs are not evidence of membership.

## Downgrade and recovery

New binaries with the option disabled reject graph reads of completed cells and
writes to cells carrying migration readiness. Older binaries cannot enforce this
guard: do not return them to serving traffic or writer credentials after activation.
Turning off the option is not a downgrade procedure. Restore a pre-migration backup
under coordinated maintenance ownership to return to legacy semantics.

The public presence-aware accessor `vertex_metadata_if_exists` returns
`Some(empty)` versus `None` and inherits scoped storage snapshots. Malformed
canonical bytes produce a corruption error. Replay retains existing idempotency
results and never recreates a vertex deleted after an earlier successful edge write.
`GraphSnapshot::vertex_metadata_if_exists` reads the snapshot retained by the
caller, including after later writes or deletion.
